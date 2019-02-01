package com.entrobus.test

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.{DataStreamReader, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object readText {
    def main(args:Array[String]):Unit= {
        val spark: SparkSession = SparkSession.builder()
          .appName("readText")
          .master("local[2]")
          .getOrCreate()
        val sc: SparkContext = spark.sparkContext
        sc.setLogLevel("WARN")


        import spark.implicits._
//        import org.apache.spark.sql.functions._
        val df1: DataFrame = spark.read
          .format("csv")
          .option("inferSchema", true)
          .option("header",true)
          .option("path", "E:\\test\\csv\\smart")
          .load()
        val schema = df1.schema



        val df2: DataFrame = spark.readStream
          .format("csv")
          .schema(schema)
          .option("header",true)
          .option("path", "E:\\test\\csv\\smart")
            .load()

        df2.printSchema()
        val df3 = df2
        val bd: Broadcast[DataFrame] = sc.broadcast(df3)
        implicit val myEncoder= org.apache.spark.sql.Encoders.javaSerialization[Row]
        val df4: Dataset[Row] = df2.mapPartitions(x => {
            val df2_0: DataFrame = bd.value
            df2_0.show()
            x
        })

        df4.writeStream
          .outputMode("append")
          .format("console")
          .start()
          .awaitTermination()

    }

}
