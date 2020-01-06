
package com.tcp.log

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

object LogParser {
 
  def main(args: Array[String]) {

    //1. membuat streaming context dengan ukuran batch 1
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))
    
    setupLogging()
    
    // 2.mengambil pattern dari satu weblog IP, Client, urls
    val pattern = apacheLogPattern()

    // 3.membuat socket stream untuk membaca dari netcat secara local
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    //4.cek apakah suatu line sama dengan pola
    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})
    
    // 5.mengambil url dari suatu pola
    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})
    
    // 6.Reduce Url dengan sliding windows 5 menit
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    
    // Urutkan dari yang terbesar
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()
    
    // Jalankan Streaming
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

