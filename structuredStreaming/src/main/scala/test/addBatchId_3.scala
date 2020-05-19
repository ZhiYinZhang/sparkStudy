package test

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.streaming.StreamingQueryProgress
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
/**
  * 在structuredStreaming中动态更新广播变量
  */
object addBatchId_3 {
  def main(args:Array[String]):Unit={
     val spark=SparkSession.builder().appName("broadcast update for structuredStreaming")
//       .master("local[*]")
       .getOrCreate()
     import spark.implicits._
     val sc=spark.sparkContext
     sc.setLogLevel("WARN")

     //广播变量初始化
     var bd: Broadcast[Long]=sc.broadcast(0)
    //函数
     val  add_batchid=()=>{
          bd.value
     }
    //方法
    def add_batchid1()={
      bd.value
    }

    val add_batchid_udf=udf(add_batchid,LongType)
    //将方法转成函数
//    val add_batchid_udf=udf(add_batchid1 _,LongType)


     val streamDF: DataFrame = spark.readStream
       .format("rate")
       .option("rowsPerSecond", 2)
       .load()
     streamDF.printSchema()

     val streamDF2=streamDF.withColumn("batchId",add_batchid_udf())
     streamDF2.printSchema()

     val query=streamDF2.writeStream
       .outputMode("append")
       .format("console")
       .option("truncate",false)
       .option("numRows",1)
//       .trigger(Trigger.ProcessingTime(3,TimeUnit.SECONDS))
       .start()

     var batchId=(-1).toLong
     while(true){
       val progress: StreamingQueryProgress = query.lastProgress
       if(progress !=null){
         if(progress.batchId>batchId & progress.numInputRows>0){
           batchId=progress.batchId
           bd.unpersist()
           //在这里广播batchId时，这个batchId已经处理了
           bd=sc.broadcast(batchId+1)
           println(batchId)
         }
       }
     }
   }

}
