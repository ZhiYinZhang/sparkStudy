package com.entrobus.windowOperator

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery

object waterMark {
 def main(args:Array[String]):Unit={
    val spark:SparkSession=SparkSession.builder()
     .appName("waterMark")
      .master("local[1]")
     .getOrCreate()

   import spark.implicits._
   val windowDuration=4
   val slideDuration=2
   spark.sparkContext.setLogLevel("WARN")

   val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
   val timestamp: String = sdf.format(System.currentTimeMillis())


   val lines: DataFrame = spark.readStream
     .format("text")
     .option("includeTimestamp", true)
     .load("E:\\Entrobus\\txt")
      .withColumn("timestamp",$"value")

   val words: DataFrame = lines.as[(String, Timestamp)]
     .flatMap(line => line._1.split(" ").map(word => (word, line._2)))
     .toDF("word", "timestamp")
   val windowCounts: Dataset[Row] = words.groupBy(window($"timestamp", s"$windowDuration seconds", s"$slideDuration seconds"), $"word")
     .count().orderBy("window")


// windowCounts.printSchema()
//   println(CurrentTimestamp())

lines.printSchema()

   val query: StreamingQuery = lines.writeStream
     .outputMode("append")
//     .option("checkpointLocation","e:\\Entrobus\\checkpoint")
//     .option("path","e:\\Entrobus\\output4")
     .format("console")
    .option("truncate", false)
     .start()
   query.awaitTermination()
 }

}
