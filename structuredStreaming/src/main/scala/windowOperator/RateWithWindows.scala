package windowOperator
import generateData._
import java.io.File

import org.apache.commons.io.FileUtils
import java.sql.Timestamp
import java.util
import java.util.{Random, UUID}


import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql._

import scala.collection.mutable.Map
object RateWithWindows {
  def main(args:Array[String]):Unit={

      val rows: Int = 1
      val checkpointLocation="e:\\test\\checkpoint2"
      val outputPath="e:\\test\\output2"

      val file1 = new File(outputPath)
      val file2 = new File(checkpointLocation)
        FileUtils.deleteDirectory(file1)
        FileUtils.deleteDirectory(file2)

    val spark:SparkSession=SparkSession.builder()
      .appName("rate")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")


    var batchId:Long=0
    //获取流查询管理对象
    val streams: StreamingQueryManager = spark.streams
    //添加一个监听
    streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("queryStart: "+new Timestamp(System.currentTimeMillis()))
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
           val processRowsPerSecond= progress.processedRowsPerSecond

        println("batchId="+batchId+"  numInputRows="+numInputRows+
          "  inputRowsPerSecond="+inputRowsPerSecond+"  processedRowsPerSecond="+processRowsPerSecond)
//        println(progress)
        //将batchId  processedRowsPerSecond写入到文件
        writeToFile(batchId+","+inputRowsPerSecond+","+processRowsPerSecond,"e://test//rate//uuid1.csv")
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println("queryStop: "+new Timestamp(System.currentTimeMillis()))
      }
    })

    val rateDF: DataFrame = spark.readStream
      .format("rate")
      .option("rowsPerSecond", rows)
//      .option("numPartitions",4)
      .load()


    val map=getMap(1000)//  userId->ip
    var userIdList:List[String]=null
    var userId:String=null
    var ip:String=null
    //增加字段batchId  user_id page_id ad_id ad_type event_type ip
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

    val filterDS: Dataset[Row] = addDF.filter(!_.get(7).equals("view"))

    //将数据根据时间窗口进行分组，并统计每个时间窗口数据条数，最后根据时间窗口倒序排序
    val resultDS: Dataset[Row] = filterDS
      .groupBy($"event_type",window($"event_time", "4 seconds", "4 seconds"))
        .count()
//      .orderBy($"window".desc)

    resultDS.printSchema()
    val query: StreamingQuery =resultDS.writeStream
      .outputMode("complete")
      .format("console")
//      .option("checkpointLocation",checkpointLocation)
//      .option("path",outputPath)
      .option("truncate",false)
      .option("numRows",300)
      .trigger(Trigger.ProcessingTime(8000))
      .start()
     query.awaitTermination()

  }
}






































