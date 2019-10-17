package com.entrobus.test

import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkContext

import scala.collection.mutable.Map
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
/**
  * 在structuredStreaming中动态更新广播变量
  */
object broadcastUpdate {
  //定义一个全局的广播变量
   var bd: Broadcast[Map[String, String]]=null
   def main(args:Array[String]):Unit={
     val spark=SparkSession.builder().appName("broadcast update for structuredStreaming")
       .master("local[*]")
       .getOrCreate()
     import spark.implicits._
     val sc=spark.sparkContext
     sc.setLogLevel("WARN")


     spark.streams.addListener(new StreamingQueryListener {
       override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
          println(event.id,event.name,event.runId)
       }
       override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
           val progress: StreamingQueryProgress = event.progress
           val json: String = progress.prettyJson
         updateBD(sc)
//           println(json)
       }
       override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
          val id: UUID = event.id
         val runId: UUID = event.runId
         println("id:",id,"runId:",runId)
       }
     })

     //广播变量应该在这里初始化
     //.....


     val streamDF: DataFrame = spark.readStream
       .format("rate")
       .option("rowsPerSecond", 10)
       .load()
     streamDF.printSchema()

     val streamDF2=streamDF.map(x=>{
       val bdValue=bd.value
       (x.getTimestamp(0),x.getLong(1),bdValue)
     })
     streamDF2.printSchema()

     streamDF2.writeStream
       .outputMode("append")
       .format("console")
       .option("truncate",false)
       .trigger(Trigger.ProcessingTime(3,TimeUnit.SECONDS))
       .start()
       .awaitTermination()





   }

  def updateBD(sc:SparkContext): Unit ={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt: Date = new Date(System.currentTimeMillis())
    val dtStr: String = format.format(dt)

    val bdVariable: Map[String, String] = Map("dt"->dtStr)
    bd = sc.broadcast(bdVariable)
  }
}
