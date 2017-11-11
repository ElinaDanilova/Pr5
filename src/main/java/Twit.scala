import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}




  object SparkTwitterStreaming {
    def main(args: Array[String]): Unit = {

      val conf = new SparkConf()
      conf.setAppName("spark-sreaming")
      conf.setMaster("local[2]")
      val sc = new SparkContext(conf)

      val ssc = new StreamingContext(sc, Seconds(1))

      // Configure your Twitter credentials
      val apiKey = "cXjAFx7kJBLN1lQZ2509TBDQa"
      val apiSecret = "0SQjcfXuC9aqUxPbsStZtBr8Miym5KBXgmfrxGCOPn1xIJ7G9a"
      val accessToken = "924172102676172800-IWoJJGyuQygqdqSsBlQRavoke5DxnZw"
      val accessTokenSecret = "sBPOKJtDZErcVIhBS4PzhvYMjloxYHgHgF6iQRbiCsyZS"

      System.setProperty("twitter4j.oauth.consumerKey", apiKey)
      System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
      System.setProperty("twitter4j.oauth.accessToken", accessToken)
      System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

      // Create Twitter Stream
      val stream = TwitterUtils.createStream(ssc, None)
      val tweets = stream.map(t => t.getText)
      val hash = tweets.flatMap(t=>t.split(" ")).filter(s=>s.startsWith("#"))

      hash.print()

      ssc.start()
      ssc.awaitTermination()
    }
  }

