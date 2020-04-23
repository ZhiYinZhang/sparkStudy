package continous

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object test {
        def main(args:Array[String]):Unit={

          val spark: SparkSession = SparkSession.builder()
            .appName("test")
            .master("local[1]")
            .getOrCreate()
          import spark.implicits._
          spark.sparkContext.setLogLevel("WARN")

          val rateDF: DataFrame = spark.readStream
            .format("rate")
            .option("rowsPerSecond", 1)
            .load()
          val rateDS: Dataset[(Timestamp, Long)] = rateDF.as[(Timestamp,Long)]
          val resultDS: Dataset[String] = rateDS.map(x => {
            println(x)
            x._1.toString()+x._2
          })

          resultDS.selectExpr("").writeStream
            .outputMode("append")
            .format("kafka")
            .option("kafka.bootstrap.servers","10.18.0.15:9193,10.18.0.19:9193,10.18.0.26:9193")
            .option("topic","rate")
            .option("checkpointLocation","E:\\test\\checkpoint1")
            .trigger(Trigger.Continuous(1000))
            .start()
            .awaitTermination()









        }
}
