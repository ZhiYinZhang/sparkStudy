package com.entrobus.MicroBatchVSContinous

import java.sql.Timestamp

import com.entrobus.generateData._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object continuousTrigger {
  def main(args:Array[String]):Unit= {

    val rows = 100000
    //获取sparkSession对象
    val spark: SparkSession = SparkSession.builder
      .appName("continuousTrigger")
      .master("local[2]")
      .getOrCreate()

    //设置日志输出级别
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    var batchId: Long = 0
    //对查询添加一个监听，获取每个批次的处理信息
    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val progress: StreamingQueryProgress = event.progress
        batchId = progress.batchId
        val inputRowsPerSecond: Double = progress.inputRowsPerSecond
        val processRowsPerSecond: Double = progress.processedRowsPerSecond
        val numInputRows: Long = progress.numInputRows
        println("batchId=" + batchId, "  numInputRows=" + numInputRows + "  inputRowsPerSecond=" + inputRowsPerSecond +
          "  processRowsPerSecond=" + processRowsPerSecond)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })


    //读取数据源
    val rateDF: DataFrame = spark.readStream
      .format("rate")
      .option("rowsPerSecond", rows)
      .load()


    val map=getMap(1000)//  userId->ip
    var userIdList:List[String]=null
    var userId:String=null
    var ip:String=null
    //增加字段user_id page_id ad_id ad_type event_type event_time ip
    val addDS = rateDF.as[(Timestamp, Long)].map(oldTuple=>{
      userIdList=map.keySet.toList
      userId=getRandomUserId(userIdList)
      ip=map.get(userId).get
      val tuple = (oldTuple._1, oldTuple._2, batchId + 1,
        userId, getUUID(), getUUID(), randomData(5),randomData(3),ip )
      tuple
    })
    val addDF: DataFrame = addDS.toDF("event_time", "num","batchId",
      "user_id","page_id","ad_id","ad_type","event_type","ip")


   /* val value: Dataset[Row] = addDF.mapPartitions(x => {
      var str=""
      while(x.hasNext){
        str+=x.next()
      }
    })
    value.printSchema()*/
//    val filterDS: Dataset[Row] = addDF.filter(!_.get(7).equals("view"))
      val filterDS:Dataset[Row]=addDF.filter(_.get(7).equals("view"))
    //将数据根据时间窗口进行分组，并统计每个时间窗口数据条数，最后根据时间窗口倒序排序
    val resultDS: Dataset[Row] = filterDS
      //      .withWatermark("timestamp","4 seconds")
      .groupBy(window($"event_time", "4 seconds", "4 seconds"),$"event_type")
      .count()
      .orderBy($"window".desc)

    filterDS.writeStream
      .outputMode("append")
      .format("kafka")
      .option("checkpointLocation","e:\\test\\checkpoint")
      .option("kafka.bootstrap.server","10.18.0.15:9193,10.18.0.19:9193,10.18.0.26:9193")
      .option("topic","process")
      .trigger(Trigger.ProcessingTime(0))
      .start()
      .awaitTermination()
  }







}
