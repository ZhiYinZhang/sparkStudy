package continous

import java.sql.Timestamp
import java.util.Properties

import generateData.readProperties
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object readKafkaTest {
  def main(args:Array[String]):Unit={
    val spark: SparkSession = SparkSession.builder()
      .master("local[10]")
      .appName("readKafka")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","10.18.0.15:9193,10.18.0.19:9193,10.18.0.26:9193")
      .option("subscribe", "rate")
      .load()
    kafkaDF.printSchema()

    val selectDF: DataFrame = kafkaDF.selectExpr("cast(value as string)","cast(timestamp as string)").selectExpr("value","cast(timestamp as timestamp)")

    selectDF.writeStream
      .outputMode("append")
      .format("console")
//      .option("checkpointLocation",checkpointLocation)
//      .option("path",resultDataPath)
      .option("truncate",false)
      .option("numRows",1000)
      .start()
      .awaitTermination()
  }
}
