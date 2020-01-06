
package com.tcp.log

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

import java.util.concurrent._
import java.util.concurrent.atomic._


object LogAlarmer {
  
  def main(args: Array[String]) {

    // Membuat streaming context dengan ukuran 1 menit
    val ssc = new StreamingContext("local[*]", "LogAlarmer", Seconds(1))
    
    setupLogging()
    
    // membuat pattern dari suatu log
    val pattern = apacheLogPattern()

    // Memnbuat socket stream yang akan menangkap data dari port 9999
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    // mengambil status dari suatu lines
    val statuses = lines.map(x => {
          val matcher:Matcher = pattern.matcher(x); 
          if (matcher.matches()) matcher.group(6) else "[error]"
        }
    )
    
    // Map tingkat kegagalan
    val successFailure = statuses.map(x => {
      val statusCode = util.Try(x.toInt) getOrElse 0
      if (statusCode >= 200 && statusCode < 300) {
        "Success"
      } else if (statusCode >= 500 && statusCode < 600) {
        "Failure"
      } else {
        "Other"
      }
    })
    
    // hitung kegagalan pada sliding windows 5 menit
    val statusCounts = successFailure.countByValueAndWindow(Seconds(300), Seconds(1))
    
    // Untuk setiap batch ambil RDD di window saat ini
    statusCounts.foreachRDD((rdd, time) => {
      
      var totalSuccess:Long = 0
      var totalError:Long = 0

      if (rdd.count() > 0) {
        val elements = rdd.collect()
        for (element <- elements) {
          val result = element._1
          val count = element._2
          if (result == "Success") {
            totalSuccess += count
          }
          if (result == "Failure") {
            totalError += count
          }
        }
      }

      //print tingkat kesuksesan dan kegagalan pada suatu windows
      println("Total success: " + totalSuccess + " Total failure: " + totalError)
      
      // Hanya memberi peringatan ketika rasio error dan sukses >0.5
      if (totalError + totalSuccess > 100) {
 
        val ratio:Double = util.Try( totalError.toDouble / totalSuccess.toDouble ) getOrElse 1.0
        if (ratio > 0.5) {
          // pada kasus nyata bisa menggunakan JavaMail atau scala's courier library
          // untuk mengirim email kepada orang yang bertanggun jawab
        } else {
          println("All systems go.")
        }
      }
    })
    
    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

