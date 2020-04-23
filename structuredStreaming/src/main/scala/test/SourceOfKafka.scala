package test

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SourceOfKafka {
 def main(args:Array[String]):Unit={
    val spark: SparkSession = SparkSession.builder().appName(" KafkaRedisAdvertisingStream").getOrCreate()
    import spark.implicits._
   spark.sparkContext.setLogLevel("WARN")

   val lineDF:DataFrame=spark.readStream
     .format("kafka")
     .option("kafka.bootstrap.servers","10.18.0.15:9092,10.18.0.17:9092,10.18.0.19:9092")
     .option("subscribe","ad-events")
     .load()

   val result: Dataset[String] = lineDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
     .as[(String, String)].map(_._2)

  val query:StreamingQuery=result.writeStream
     .outputMode("append")
    .format("text")
    .option("checkpointLocation","hdfs://10.18.0.15:8020/user/hadoop/benchmark/checkpoint")
    .option("path","hdfs://10.18.0.15:8020/user/hadoop/benchmark/spark-kafka")
     .start()
   query.awaitTermination()

 }
}
