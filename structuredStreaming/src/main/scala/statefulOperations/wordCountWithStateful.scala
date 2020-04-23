package statefulOperations


import java.sql.Timestamp

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, StreamingQueryListener}
import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, SparkSession}

/**
  * 状态：即当前计算的结果和以前的数据有关(即要用到以前的数据)，聚合操作：sum，count，max，min等
  */
object wordCountWithStateful {
  def main(args:Array[String]):Unit={
    val spark: SparkSession = SparkSession.builder()
      .appName("wordCount")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._


    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val progress = event.progress
        println(progress.eventTime)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ???
    })


    val lines: DataFrame = spark.readStream
      .format("delta")
      .load("e://test//delta//test")

//    val splitDS: Dataset[(String, Timestamp)] = lines.as[(String,Timestamp)].flatMap(x=>{x._1.split(" ").map((_,x._2))})

    val result=lines.toDF("word","timestamp").withWatermark("timestamp","3 days")
        .groupBy("word").count()


    result.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate",false)
      .start()
      .awaitTermination()

  }
  //把状态封装成一个样例类
  case class ValueState(word:String,num:Long,startTimestamp:Timestamp,endTimestamp:Timestamp,processTime:Timestamp,timeout:Boolean)
}
