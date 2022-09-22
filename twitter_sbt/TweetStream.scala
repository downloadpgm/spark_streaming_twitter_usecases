package TweetStream

// import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object tweetstream {

   def main( args: Array[String] ) {

      // val spark = SparkSession.builder().appName("twitter consumer").getOrCreate()
      // val sc = spark.sparkContext
      val conf = new SparkConf().setAppName("twitter consumer")
      val sc = new SparkContext(conf)

      val builder = new ConfigurationBuilder()
      builder.setDebugEnabled(true)
      builder.setOAuthConsumerKey("<ConsumerKey>")
      builder.setOAuthConsumerSecret("<ConsumerSecret>")
      builder.setOAuthAccessToken("<AccessToken>")
      builder.setOAuthAccessTokenSecret("<AccessTokenSecret>")

      val auth = new OAuthAuthorization(builder.build)

      val filters = Array("trump", "donald")

      val ssc = new StreamingContext(sc,Seconds(10))
      val twitterStream = TwitterUtils.createStream(ssc, Some(auth), filters )

      val tweets = twitterStream.filter(tweet => tweet.getLang.equals("en") || tweet.getLang.equals("")).map(_.getText()).map(_.replaceAll("/[^A-Za-z0-9 ]/", "")).map(_.replaceAll("/", "")).map(_.replaceAll("RT.+?(?=\\s)\\s", "")).map(_.replaceAll("https([^\\s]+).*", ""))

      tweets.repartition(1).foreachRDD( rdd => {
        val topList = rdd.take(10)
        topList.foreach(println)
        })

      ssc.checkpoint("hdfs://<hdp-mst>:9000/user/hduser/checkpoint")   // used when master = yarn
	  // sc.setCheckpointDir("hdfs://<hdp-mst>:9000/user/hduser/checkpoint")     // used when master = spark std alone

      ssc.start()
      ssc.awaitTermination()
   }
}
