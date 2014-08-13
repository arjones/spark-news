package io.smx.spark.news

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.streaming.Minutes
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object RecommendLinksDrive {

  val sc = new SparkContext()

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val tweetsRDD = createDataset(sqlContext, "./urls", 1407962160, 1407963900)

  tweetsRDD.registerAsTable("tweets")
  sqlContext.cacheTable("tweets")
  
  // Show the SchemaRDD
  tweetsRDD.printSchema()

  import sqlContext._

  val tweetsCount = sql("SELECT COUNT(*) FROM tweets")
  println(f"\n\n\nThere are ${tweetsCount.collect.head.getLong(0)} Tweets on this Dataset\n\n")

  val topLinks = sql("SELECT link, COUNT(*) AS ct FROM tweets GROUP BY link ORDER BY ct DESC LIMIT 10")
  prettyResults(topLinks.collect)
  
  // ------------------------------
  
  
  def folderExists(basePath: String, id: Int) = new java.io.File(basePath + "/" + id).exists()

  def createDataset(sqlContext: SQLContext, basePath: String, from: Int, to: Int): SchemaRDD = {
    val minute1 = 60 // 1min in seconds
    val rddId = Range(from, to + 1, minute1)

    val schemaRDDs = rddId.filter(folderExists(basePath, _)).map { id =>
      sqlContext.parquetFile(basePath + "/" + id)
    }

    schemaRDDs.reduce((s1, s2) => s1.unionAll(s2))
  }
  
  def prettyResults(rows: Array[org.apache.spark.sql.Row]) {
    println("\n\nLinks most shared:")
    println("  Count\tLink")
    
    rows.foreach(r =>{
      val count = r.getLong(1)
      val url = r.getString(0)

      println(f"  $count\t$url")
    })
      
    println("\n\n")
  }

}