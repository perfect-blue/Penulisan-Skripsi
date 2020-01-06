package com.sparkstreaming.twitter

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._


object SaveTweets {
  

  def main(args: Array[String]) {

    setupTwitter()
    
    //membuath batch interval ukuran 1 detik
    val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(1))
    
    setupLogging()

    // Membuat aliran data
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Ambil text status
    
    val statuses = tweets.map(status => status.getText())
    val names=tweets.map(user=>user.getUser().getName())
    
    statuses.saveAsTextFiles("hdfs://localhost:50071/Twitter/Status/Output", "txt")

	ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
