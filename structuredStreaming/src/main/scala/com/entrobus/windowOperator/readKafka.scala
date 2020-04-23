package com.entrobus.windowOperator
import com.entrobus.generateData._
import java.sql.Timestamp
import java.util.{Random, UUID}

import org.apache.spark.sql._
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming._

import scala.collection.mutable.Map

object readKafka {
    def main(args:Array[String]):Unit={

      val spark:SparkSession=SparkSession.builder()
        .appName("readKafka")
        .master("local[2]")
        .getOrCreate()


      SparkSession.builder().getOrCreate()
      import spark.implicits._
      spark.sparkContext.setLogLevel("WARN")

      //获取StreamingQueryManager对象
      val streams: StreamingQueryManager = spark.streams
      //通过StreamingQueryManager#addListener方法添加一个监听
      var batchId:Long=0
      streams.addListener(new StreamingQueryListener {
        override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        }
        override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
          val progress: StreamingQueryProgress = event.progress

          //每个batch的id
           batchId= progress.batchId
          //每个batch处理的数据量
          val numInputRows: Long = progress.numInputRows
          //数据输入速度
          val inputRowsPerSecond: Double = progress.inputRowsPerSecond
          //处理数据速度
          val processedRowsPerSecond: Double = progress.processedRowsPerSecond
          val sources: Array[SourceProgress] = progress.sources

          println("batchId="+batchId+"  numInputRows="+numInputRows+
            "  inputRowsPerSecond="+inputRowsPerSecond+"  processedRowsPerSecond="+processedRowsPerSecond)
                  println(progress)
        }
        override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        }
      })

//      |-- key: binary (nullable = true)
//      |-- value: binary (nullable = true)
//      |-- topic: string (nullable = true)
//      |-- partition: integer (nullable = true)
//      |-- offset: long (nullable = true)
//      |-- timestamp: timestamp (nullable = true)
//      |-- timestampType: integer (nullable = true)
      val kafkaDF: DataFrame = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "10.18.0.15:9193,10.18.0.19:9193,10.18.0.26:9193")
        .option("subscribe","rate")
        .load()

      kafkaDF.printSchema()

//      |-- key: timestamp (nullable = true)
//      |-- value: string (nullable = true)
      val topicDF: DataFrame = kafkaDF.selectExpr("cast(key as string)","cast(value as String)").selectExpr("cast(key as timestamp)","value")

      val map=getMap(1000)//  userId->ip
      var userIdList:List[String]=null
      var userId:String=null
      var ip:String=null
      //增加字段user_id page_id ad_id ad_type event_type event_time ip
      val addDS = topicDF.as[(Timestamp,String)].map(oldTuple=>{
        userIdList=map.keySet.toList
        userId=getRandomUserId(userIdList)
        ip=map.get(userId).get
        val tuple = (oldTuple._1, oldTuple._2, batchId + 1,
          userId, getUUID(), getUUID(), randomData(5),randomData(3),ip )
        tuple
      })
      val addDF: DataFrame = addDS.toDF("event_time", "num","batchId",
        "user_id","page_id","ad_id","ad_type","event_type","ip")
      val filterDS: Dataset[Row] = addDF.filter(!_.get(7).equals("view"))


      //将数据根据时间窗口进行分组，并统计每个时间窗口数据条数，最后根据时间窗口倒序排序
      val resultDS: Dataset[Row] = filterDS
        .groupBy(window($"event_time", "4 seconds", "4 seconds"),$"event_type")
        .count()
        .orderBy($"window".desc)

      val query: StreamingQuery =resultDS.writeStream
        .outputMode("complete")
        .format("console")
              .option("checkpointLocation","e:\\Entrobus\\checkpoint1")
             .option("path","e:\\Entrobus\\output1")
        .option("truncate",false)
        .option("numRows",3000)
        .trigger(Trigger.ProcessingTime(8000))
//        .trigger(Trigger.Continuous(1000))
        .start()
         query.awaitTermination()

    }

}
