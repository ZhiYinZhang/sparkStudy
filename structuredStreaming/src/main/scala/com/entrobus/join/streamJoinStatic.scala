package com.entrobus.join

import java.sql.Timestamp
import java.util.Random

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}




object streamJoinStatic {
      def main(args:Array[String]):Unit={

            val spark: SparkSession = SparkSession.builder()
              .appName("streamWithStatic")
              .master("local[2]")
              .getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            import spark.implicits._
            //获取静态数据
            val staticSource: Dataset[String] = spark.read.textFile("e://test//course.txt")
            val staticDS: Dataset[(Int, String, String)] = staticSource.map(x => {
                  val arr: Array[String] = x.split(" ")
                  val tuple = (arr(0).toInt, arr(1), arr(2))
                  tuple
            })
            val staticDF: DataFrame = staticDS.toDF("course_id","course","teacher")


            //获取动态数据
            val streamSource: Dataset[(Timestamp, Long)] = spark.readStream
              .format("rate")
              .option("rowsPerSecond", 10)
              .load()
              .as[(Timestamp, Long)]
            val streamDS: Dataset[(Int, String, Int)] = streamSource.map(x => {
                  getData()
            })
            val streamDF: DataFrame = streamDS.toDF("studentNum","student","course_id")


            val frame: DataFrame = streamDF.join(staticDF,Seq("course_id"))
            frame.printSchema()

            val query: StreamingQuery = frame.writeStream
//              .outputMode("complete")
              .format("console")
              .option("truncate", false)
              .option("numRows", 100)
              .trigger(Trigger.ProcessingTime(1000))
              .start()
            query.awaitTermination()

      }
      def getData():(Int,String,Int)={
            val arr = Array((1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E"),
                  (6, "F"), (7, "G"), (8, "H"), (9, "I"), (10, "J"),
                  (11, "K"), (12, "L"), (13, "M"), (14, "N"), (15, "O"),
                  (16, "P"), (17, "Q"), (18, "R"), (19, "S"), (20, "T"),
                  (21, "U"), (22, "V"), (23, "W"), (24, "X"), (25, "Y"),
                  (26, "Z"))
            val random: Random = new Random()
            val result: Array[(Int, String, Int)] = arr.map(x => {
                  (x._1, x._2, random.nextInt(10) + 1)
            })
            val i: Int = random.nextInt(arr.length)
            result(i)
      }
}
