package com.sparkstreaming.twitter

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._


object PopularHashtags {
  
  def main(args: Array[String]) {

    // Setting twitter Credentials
    setupTwitter()
    
    //setup streaming context ukuran 1 detik
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    
    // hapus spam selain error
    setupLogging()

    // Membuat Dstream dengan Streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // ambil status
    val statuses = tweets.map(status => status.getText())
    
    // ambil setiap kata
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    
    // filter yang bukan hashtag
    val hashtags = tweetwords.filter(word => word.startsWith("#"))
    
    // Map setiap hastag menjadi key value (hashtag,1)
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    
    //Hitung hashtag perdetik dengan sliding window selama 5 menit
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(5))
    
    // Sort berdasarkan banyak hashtag
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    
    // Print the top 10
	sortedResults.saveAsTextFiles("hdfs://localhost:50071/Twitter/Hashtag/Output1", "txt")
    sortedResults.print
    
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
