package continous

import generateData._
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object readKafka {
        def main(args:Array[String]):Unit={
          val prop: Properties = readProperties("D:\\IdeaProjects\\demo\\structuredStreaming\\src\\test.properties")
          val kafkaHost: String = prop.getProperty("kafkaHost")
          val topic: String = prop.getProperty("topic")
          val resultDataPath: String = prop.getProperty("resultDataPath")
          val checkpointLocation=prop.getProperty("checkpointLocation_kafka")
          if(kafkaHost==null || topic==null || resultDataPath==null){
            throw new NoSuchElementException("参数名称错误！！！需要如下参数:kafkaHost,topic,resultDataPath")
          }

          val spark: SparkSession = SparkSession.builder()
            .master("local[10]")
            .appName("readKafka")
            .getOrCreate()
          import spark.implicits._
          spark.sparkContext.setLogLevel("WARN")

          val kafkaDF: DataFrame = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers",kafkaHost)
            .option("subscribe", topic)
            .load()
          kafkaDF.printSchema()

          val selectDF: DataFrame = kafkaDF.selectExpr("cast(value as string)","cast(timestamp as string)")
            .selectExpr("value","cast(timestamp as timestamp)")
          val addDS: Dataset[(String, Timestamp, Timestamp,Long)] = selectDF.as[(String, Timestamp)].map(x => {
//            Thread.sleep(2000)
            val str: String = x._1.split("\t")(0)
            val delayMs=x._2.getTime()-Timestamp.valueOf(str).getTime()
            writeToFile(delayMs.toString,resultDataPath)
            val tuple: (String, Timestamp, Timestamp,Long) = (x._1, x._2,new Timestamp(System.currentTimeMillis()),delayMs)
            tuple
          })
          val resultDF: DataFrame = addDS.toDF("value","kafkaTimestamp","currentTimestamp","delayMs")
          resultDF.writeStream
              .outputMode("append")
            .format("console")
//            .option("checkpointLocation",checkpointLocation)
//            .option("path","e:\\test\\continuous\\continuous")
            .option("truncate",false)
            .start()
            .awaitTermination()
        }
}
