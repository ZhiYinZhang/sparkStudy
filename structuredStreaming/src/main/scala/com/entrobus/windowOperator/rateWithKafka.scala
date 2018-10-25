package com.entrobus.windowOperator

import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, StreamingQueryProgress}
import org.apache.spark.sql.{DataFrame, SparkSession}

object rateWithKafka {
    def main(args:Array[String]):Unit={

      val rows=5
      val spark: SparkSession = SparkSession.builder()
        .appName("rateWithKafka")
        .master("local[1]")
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")


      //对查询添加一个监听，获取每个批次的处理信息
      spark.streams.addListener(new StreamingQueryListener() {
        override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}
        override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
          val progress: StreamingQueryProgress = event.progress
          val batchId = progress.batchId
          val inputRowsPerSecond: Double = progress.inputRowsPerSecond
          val processRowsPerSecond: Double = progress.processedRowsPerSecond
          val numInputRows: Long = progress.numInputRows
          println("batchId=" + batchId, "  numInputRows=" + numInputRows + "  inputRowsPerSecond=" + inputRowsPerSecond +
            "  processRowsPerSecond=" + processRowsPerSecond)
        }
        override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
      })
      /*rateDF
      |-- timestamp: timestamp (nullable = true)
      |-- value: long (nullable = true) */
      val rateDF: DataFrame = spark.readStream
        .format("rate")
        .option("rowsPerSecond", rows)
        .load()
      /*resultDF
      |-- key: timestamp (nullable = true)
      |-- value: string (nullable = true)*/
      val resultDF: DataFrame = rateDF.toDF("key","value")

      val query: StreamingQuery =resultDF
        .selectExpr("cast(key as string)","CAST(value as String)")
        .writeStream
        .format("kafka")
        .option("checkpointLocation","e://Entrobus//checkpoint")
        .option("kafka.bootstrap.servers", "10.18.0.15:9092,10.18.0.20:9092,10.18.0.19:9092")
        .option("topic", "rate1")
        .start()
      query.awaitTermination()


    }
}
