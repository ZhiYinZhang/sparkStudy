package test

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.Trigger

object writeToFile {
  def main(args:Array[String]):Unit={

    val spark: SparkSession = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val rateDF: DataFrame = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10000)
      .load()
    val rateDS: Dataset[(Timestamp, Long)] = rateDF.as[(Timestamp,Long)]
    val resultDS: Dataset[String] = rateDS.map(x => {
//      println(x)
      x._1.toString()+""+x._2
    })

    rateDF.writeStream
      .outputMode("append")
      .format("csv")
      .option("checkpointLocation","E:\\test\\checkpoint1")
      .trigger(Trigger.ProcessingTime(1000))
      .start("e:\\test\\csv1")
      .awaitTermination()
  }
}
