package com.entrobus.statefulOperations

import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}

object statefulOperation {
    def main(args:Array[String]):Unit={
      val spark: SparkSession = SparkSession.builder()
        .master("local[2]")
        .appName("statefulOperation")
        .getOrCreate()
      import spark.implicits._
      spark.sparkContext.setLogLevel("WARN")

      var batchId:Long=0
      //设置query的listener
      spark.streams.addListener(new StreamingQueryListener {
        override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
          println("query start:"+new Timestamp(System.currentTimeMillis()))
        }
        override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
          val progress: StreamingQueryProgress = event.progress
          batchId=progress.batchId
        }
        override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
      })

      val rateDF: DataFrame = spark.readStream
        .format("rate")
        .option("rowsPerSecond", 1)
        .load()
      val addDF: DataFrame = rateDF.as[(Timestamp, Long)].map(x => {
        (x._1, x._2, batchId)
      }).toDF("eventTime", "value", "batchId")

     /* val mappingFunc=(batchId:Long,values:Iterator[(Timestamp,Long)],state:GroupState[Long])=>{
        val newState: Long = state.getOption.map(_+values.size).getOrElse(0L)
        state.update(newState)
        batchId
      }*/
   /* def mappingFunc(batchId:Long,values:Iterator[(Timestamp,Long)],state:GroupState[Long]){
       val newState: Long = state.getOption.map(_+values.size).getOrElse(0L)
       state.update(newState)
       batchId
     }*/

//      addDF.groupByKey(_.get(2))





     addDF.printSchema()


     addDF.writeStream
          .outputMode("append")
        .format("console")
        .option("truncate",false)
        .start()
        .awaitTermination()
    }





}
