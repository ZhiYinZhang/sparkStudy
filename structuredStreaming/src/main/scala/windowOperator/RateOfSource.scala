package windowOperator

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, StreamingQueryProgress}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
object RateOfSource {
  def main(args:Array[String]):Unit={

    val path= "E:\\Entrobus\\"
    val checkpoint=path+"checkpoint"
    val outputPath=path+"filter"
    //数据生成速率 每秒rows行数据
    val rows=1000000

    val spark:SparkSession=SparkSession.builder
      .appName("SourceOfRate")
      .master("local[1]")
      .getOrCreate()
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("queryStart:"+new Timestamp(System.currentTimeMillis()))
      }
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val progress: StreamingQueryProgress = event.progress
        val batchId: Long = progress.batchId
        val numInputRows: Long = progress.numInputRows
        val inputRowsPerSecond: Double = progress.inputRowsPerSecond
        val processedRowsPerSecond: Double = progress.processedRowsPerSecond
        println("batchId="+batchId+"  numInputRows="+numInputRows+"  inputRowsPerSecond="+inputRowsPerSecond+"  processedRowsPerSecond="+processedRowsPerSecond)
        val data=batchId+","+numInputRows+","+inputRowsPerSecond+","+processedRowsPerSecond
        writeToFile(data,path+"filter.csv")
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit ={}
    })


    val rateDF: DataFrame = spark.readStream
      .format("rate")
      .option("rowsPerSecond",rows)
      .load().toDF("timestamp","num")

    val timeDS: Dataset[(Timestamp, Long, String, String, String, String)] = rateDF.as[(Timestamp, Long)].map(x => {
      val arr: Array[String] = dateFormat(System.currentTimeMillis())
      val tuple = (x._1, x._2, arr(3), arr(4), arr(5), arr(6))
      tuple
    })

    val timeDF: DataFrame = timeDS.toDF("timestamp", "num", "hour", "minute", "second", "milli")
    val filterDS: Dataset[Row] = timeDF.filter("num%2=0")
//    timeDF.createOrReplaceTempView("temp")
//    val countDF: DataFrame = spark.sql("select hour,minute,second,count(1) count from temp group by hour,minute,second order by minute desc,second desc")
    val aggDS: Dataset[Row] = timeDF
    .withWatermark("timestamp","4 seconds")
      .groupBy(window($"timestamp", "4 seconds", "4 seconds"))
      .count()
//      .orderBy($"window".desc)

    val query:StreamingQuery=aggDS.writeStream
      .outputMode("append")
      .format("json")
//      .option("truncate",false)
//      .option("numRows",10000)
//      .trigger(Trigger.ProcessingTime("5 second"))
       .option("checkpointLocation",checkpoint)
       .option("path",outputPath)
      .start()

    query.awaitTermination()


  }
  def dateFormat(timestamp:Long):Array[String]={
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy:MM:dd:hh:mm:ss:SSS")
    val str: String = sdf.format(timestamp)
    val arr: Array[String] = str.split(":")
    arr
  }
  def writeToFile(data:String,dest:String): Unit ={
    val destDir = new File(dest)
    FileUtils.writeStringToFile(destDir,data+"\n",true)
  }
}
