package join

import java.sql.Timestamp
import java.util.Random

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._
object streamJoinStream {
      def main(args:Array[String]):Unit={

        val spark: SparkSession = SparkSession.builder()
          .appName("streamWithStatic")
          .master("local[2]")
          .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        import spark.implicits._

        //获取流查询管理对象
        val streams: StreamingQueryManager = spark.streams
        //添加一个监听
        streams.addListener(new StreamingQueryListener {
          override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
          }
          override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
            val progress: StreamingQueryProgress = event.progress
            println(progress)
          }
          override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
          }
        })



        //获取课程数据
        val streamSource1: Dataset[(Timestamp, Long)] = spark.readStream
          .format("rate")
          .option("rowsPerSecond", 1)
          .load()
          .as[(Timestamp, Long)]

        val streamDS1: Dataset[(Int, String,String)] = streamSource1.map(x => {
          getCourse()
        })
        val streamDF1: DataFrame = streamDS1.toDF("course_id","course","teacher")


        //获取学生数据
        val streamSource2: Dataset[(Timestamp, Long)] = spark.readStream
          .format("rate")
          .option("rowsPerSecond", 3)
          .load()
          .as[(Timestamp, Long)]

        val streamDS2: Dataset[(Int, String, Int)] = streamSource2.map(x => {
          getStudent()
        })
        val streamDF2: DataFrame = streamDS2.toDF("studentNum","student","course_id")


        val resultDF: DataFrame = streamDF2.join(streamDF1, Seq("course_id"))
           val resultDS: Dataset[Row] = resultDF.dropDuplicates()

        val query: StreamingQuery = resultDS.writeStream
          .outputMode("append")
          .format("console")
          .option("truncate", false)
          .option("numRows", 100)
//          .trigger(Trigger.ProcessingTime(1000))
          .start()


        println(query.exception)

        query.awaitTermination()



      }






  def getStudent():(Int,String,Int)={
    val arr = Array((1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E"),
      (6, "F"), (7, "G"), (8, "H"), (9, "I"), (10, "J"),
      (11, "K"), (12, "L"), (13, "M"), (14, "N"), (15, "O"),
      (16, "P"), (17, "Q"), (18, "R"), (19, "S"), (20, "T"),
      (21, "U"), (22, "V"), (23, "W"), (24, "X"), (25, "Y"),
      (26, "Z"))
    val random: Random = new Random()
    val result: Array[(Int, String, Int)] = arr.map(x => {
      (x._1, x._2, random.nextInt(12) + 1)
    })
    val i: Int = random.nextInt(arr.length)
    result(i)
  }
  def getCourse():(Int,String,String)={
    val arr = Array((1, "足球","Tom"),(2, "篮球","Jerry"),(3, "排球","Spike"),(4, "散打","Tyke")
      ,(5, "瑜伽","Butch"),(6, "游泳","Topsy"),(7, "羽毛球","Tomas"),(8, "健美操","Jinks")
      ,(9, "网球","Higgins"),(10, "健身健美","Jason"),(11, "艺术体操","Hanks"),(12, "乒乓球","Hanson"))
    val random: Random = new Random()
    val i: Int = random.nextInt(arr.length)
    arr(i)
  }
}
