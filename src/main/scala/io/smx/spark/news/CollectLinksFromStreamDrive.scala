package io.smx.spark.news

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import TutorialHelper._

// This class contains the fields definition that we'll save to Disk in Parquet Format
case class TweetContent(screenName: String, name: String, profilePic: String, followers: Long, link: String)

object CollectLinksFromStreamDrive {
  def main(args: Array[String]) {
    if (args.length != 1) {
      println("Missing argument, a following list is obligatory")
      println("\tCollectLinksFromStreamDrive <following.txt>")
      exit(1)
    }

    val filename = args(0)
    val followingList = getFollowing(filename)
    println("%s followings on %s".format(followingList.size, filename))

    // Setup the Streaming Context
    val ssc = new StreamingContext(new SparkConf(), Seconds(300))
    val tweets = TwitterUtils.createStream(ssc, None, followingList)

    // For those Tweets that contains an URL
    // extract the first one
    // and expand it (find the destination page)
    // so we can group by url
    val urlsDStream = tweets.filter(s => s.getURLEntities.size > 0).map { status =>
      
      // Consider only 1st URL on the Tweet
      val url = URLExpander.expandUrl(status.getURLEntities.head.getExpandedURL)

      // Create the case class
      TweetContent(
        status.getUser.getScreenName,
        status.getUser.getName,
        status.getUser().getProfileImageURL(),
        status.getUser.getFollowersCount(),
        url)
    }

    // Using sqlContext we have implicit conversion from RDD to SchemaRDD
    val sqlContext = new org.apache.spark.sql.SQLContext(ssc.sparkContext)
    import sqlContext._

    // Iterate through each RDD inside the DStream and save it as Parquet
    // if it contains links
    urlsDStream.foreach { rdd =>
      val links = rdd.distinct()
      val count = links.count
      println("=" * 35)
      println("%s stories shared on this RDD".format(count))
      println("=" * 35)

      // only makes sense to create a parquet file if there is data on the RDD
      if (count > 0)
        links.saveAsParquetFile("urls/" + folderTimestamp) + ".parquet"
    }

    // Checkpoint directory
    val checkpointDir = "./checkpoint/" //TutorialHelper.getCheckpointDirectory()
    println("checkpointDir is: " + checkpointDir)

    // run!
    ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()
  }

  // creates TS rounding down to minute
  def folderTimestamp = ((System.currentTimeMillis / 1000) / 60) * 60

  def getFollowing(filename: String) = scala.io.Source.fromFile(filename).getLines.toList
}

