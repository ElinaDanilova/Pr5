
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

object SparkSessionExample {
  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }
  def main(args: Array[String]): Unit = {

    val jsonFile = args(0);
    val modelOutput = "C:\\Users\\itim\\IdeaProjects\\Pr5\\kmeans"
    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    //Read json file to DF
    val tweetsDF  = sparkSession.read.json(jsonFile)

    //Show the first 100 rows
   tweetsDF.show(100);

    //Show thw scheme of DF
  //  tweetsDF.printSchema();

    //Get the features vector
     val tweet = tweetsDF.select("text")
    val textRDD = tweet.rdd.map(k=>k(0).toString)

    val features = textRDD.map(m => featurize(m))

    val numClusters = 10
    val numIterations = 40

    // Train KMenas model and save it to file
    val model: KMeansModel = KMeans.train(features, numClusters, numIterations)
    model.save(sparkSession.sparkContext, modelOutput)


  }
}