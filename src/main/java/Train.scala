import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.log4j.Logger
import org.apache.log4j.Level


object SparkTwitterStreamingKMEAN {

  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setAppName("spark-sreaming")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val outputDirectory = ".\\data\\"
    val modelInput = ".\\kmeans\\"

    // Configure your Twitter credentials
    val apiKey = "PnUMVHlODn3OxEMztaNlICbTn"
    val apiSecret = "qFvp75HqxAvpzzNZ4juD469NS4e09ytKQsgfex0volwWFrNekP"
    val accessToken = "924175535068106752-H8Mdm9jyCJUlWxNYowlGNqAfzdIY1fv"
    val accessTokenSecret = "7m4v4qxDHzGuvevH7w5QTkIU6eZNYROnvKYdKUbxkXhSY"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


    val ssc = new StreamingContext(sc, Seconds(1))

    // Create Twitter Stream
    val tweets = TwitterUtils
      .createStream(ssc, None)

    tweets.print()

    val tweetText = tweets.map(t => t.getText)

    val langNumber = 3
    val model = KMeansModel.load(sc, modelInput)

    tweetText.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val langNumber = 3
        val filtered = rdd.filter(t => model.predict(featurize(t)) == langNumber)
        filtered.foreach(println)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}