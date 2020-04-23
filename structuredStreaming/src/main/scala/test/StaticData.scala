package test

import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}



object StaticData {
  def main(args:Array[String]):Unit={

    val outPath="e:\\Entrobus\\time"
    val checkpoint="e:\\Entrobus\\checkpoint"
    val rows=100000

    val spark: SparkSession = SparkSession.builder()
      .appName("rate")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._





    val streams: StreamingQueryManager = spark.streams
    streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("queryStarted id="+event.id+" name="+event.name+" runId="+event.runId)
      }
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val progress: StreamingQueryProgress = event.progress
        val batchId: Long = progress.batchId
        val numInputRows: Long = progress.numInputRows
        val inputRowsPerSecond: Double = progress.inputRowsPerSecond
        val processedRowsPerSecond: Double = progress.processedRowsPerSecond
        val id: UUID = progress.id
        val runId: UUID = progress.runId
                    println("batchId="+batchId+"  numInputRows="+numInputRows+"  inputRowsPerSecond="+
                      inputRowsPerSecond+"  processedRowsPerSecond="+processedRowsPerSecond)
//        println("id="+id+" runId="+runId)


      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit ={
            println("queryTerminated: id="+event.id+" runId="+event.runId+" exception="+event.exception.get)
      }
    })


    val rateDF: DataFrame = spark.readStream
      .format("rate")
      .option("rowsPerSecond", rows)
      .load()
//    val spark:SparkSession=.....
    //读取文件需要指定字段类型











    val addDS: Dataset[(Timestamp, Long, Timestamp)] = rateDF.as[(Timestamp, Long)].map(oldTuple => {
      val timestamp: Timestamp = new Timestamp(System.currentTimeMillis())
      val newTuple: (Timestamp, Long, Timestamp) = (oldTuple._1, oldTuple._2, timestamp)
      newTuple
    })
    val addDF: DataFrame = addDS.toDF("timestamp", "num", "handleTimestamp")

    val query: StreamingQuery = rateDF.writeStream
      .outputMode("append")

      .format("csv")
      .option("checkpointLocation",checkpoint)
      .option("path", outPath)
      .trigger(Trigger.ProcessingTime(4000))
//      .option("truncate",false)
//      .option("numRows",1000)
      .start()

    query.awaitTermination()

  }
}
