import com.google.gson.Gson
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils


object TwitterStreamJSON {

  def main(args: Array[String]): Unit = {

    val outputDirectory = args(0)

    val conf = new SparkConf()
    conf.setAppName("spark-sreaming")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(30))

    val apiKey = "cXjAFx7kJBLN1lQZ2509TBDQa"
    val apiSecret = "0SQjcfXuC9aqUxPbsStZtBr8Miym5KBXgmfrxGCOPn1xIJ7G9a"
    val accessToken = "924172102676172800-IWoJJGyuQygqdqSsBlQRavoke5DxnZw"
    val accessTokenSecret = "sBPOKJtDZErcVIhBS4PzhvYMjloxYHgHgF6iQRbiCsyZS"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)



    // Create Twitter Stream in JSON
    val tweets = TwitterUtils
      .createStream(ssc, None)
      .map(new Gson().toJson(_))

    val numTweetsCollect = 10000L
    var numTweetsCollected = 0L


    //Save tweets in file
    tweets.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.coalesce(1)
        outputRDD.saveAsTextFile(outputDirectory+"/"+time)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsCollect) {
          System.exit(0)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}