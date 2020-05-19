package test

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQueryProgress

/**
  * 1.初始化广播变量
  * 2.在query里面获取batchId
  * 3.遍历获取lastProgress，batchid变化了就更新广播变量的值
  * 这个和第一种方法的区别就在与获取batchId，其实都是利用动态更新广播变量
  */
object addBatchId_2 {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder().appName("broadcast update for structuredStreaming")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val sc=spark.sparkContext
    sc.setLogLevel("WARN")

    //广播变量应该在这里初始化
    var bd: Broadcast[Long]=sc.broadcast(1)

    val streamDF: DataFrame = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 2)
      .load()
    streamDF.printSchema()


    val streamDF2=streamDF.map(x=>{
      val bdValue=bd.value
      (x.getTimestamp(0),x.getLong(1),bdValue)
    })
    streamDF2.printSchema()

    val query=streamDF2.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate",false)
      //       .trigger(Trigger.ProcessingTime(3,TimeUnit.SECONDS))
      .start()
    //       .awaitTermination()

    var batchId=(-1).toLong
    while(true){
      val progress: StreamingQueryProgress = query.lastProgress

      if(progress !=null){
        val curr_batchId=progress.batchId

        //batchId有变化及batch有数据(有时那个batch还没有数据，但是他也会query)
        if(curr_batchId>batchId  & progress.numInputRows>0 ){

          batchId=curr_batchId
          bd.unpersist()
          //在这里广播batchId时，这个batchId已经处理了
          bd=sc.broadcast(batchId+1)
          println(batchId)
        }
      }
    }
  }
}
