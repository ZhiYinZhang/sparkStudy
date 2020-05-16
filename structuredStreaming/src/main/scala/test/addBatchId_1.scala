package test

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}

/**
  *1.初始化广播变量
  *2.在StreamingQueryListener里面获取batchId
  *3.然后更新广播变量的值
  */
object addBatchId_1 {


  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder().appName("broadcast update for structuredStreaming")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val sc=spark.sparkContext
    sc.setLogLevel("WARN")

    //初始化广播变量
//    var bd: Broadcast[Long]=null
    var bd: Broadcast[Long]=sc.broadcast(0)

   spark.streams.addListener(new StreamingQueryListener {
     override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println(event.id,event.name,event.runId)

     }
     override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
         val progress: StreamingQueryProgress = event.progress

         val json: String = progress.prettyJson
         //执行查询时，这个batch已经结束了，所以我们应该广播的是下一个batch的id

         println(json)
         bd = sc.broadcast(progress.batchId+1)

     }
     override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        val id: UUID = event.id
       val runId: UUID = event.runId
       println("id:",id,"runId:",runId)
     }
   })


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

      query.awaitTermination()


  }

}
