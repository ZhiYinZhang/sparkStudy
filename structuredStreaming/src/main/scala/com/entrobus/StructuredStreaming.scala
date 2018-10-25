package com.entrobus

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StructuredStreaming {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder().appName("structStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))
    val result: DataFrame = words.groupBy("value").count()

    val query: StreamingQuery = result.writeStream
      .outputMode("complete")
      .format("console")

      .start()
    query.awaitTermination()

  }
}
