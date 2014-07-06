package io.smx.spark.news

object URLExpander {

  // Naive implementation of a url follower
  def expandUrl(url: String, tries: Int = 1): String = {
    println("~~ %s - %s".format(tries, url))
    import java.net.URL
    import java.net.HttpURLConnection

    // if it exceeds max retries, it's probably on infinity loop, return last URL
    if (tries > 3) return url

    var conn: HttpURLConnection = null
    try {
      conn = (new URL(url).openConnection()).asInstanceOf[HttpURLConnection]
      conn.setInstanceFollowRedirects(false)
      conn.setConnectTimeout(1000)

      if (conn.getResponseCode() / 100 == 3) {
        val location = conn.getHeaderField("location")
        expandUrl(location, tries + 1)

      } else
        url

    } catch {

      // if something goes wrong, consider the last available link
      case _: Throwable => url

    } finally {
      conn.disconnect()

    }
  }

  // TODO: testcase
  // ~~ http://onforb.es/1iNYayP
  // ~~ http://www.forbes.com/celebrities
  // ~~ /celebrities/

  //  scala> URLExpander.expandUrl("http://simplog.jp/pub/tw/15268201/53")
}