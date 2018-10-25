package com.entrobus.test

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
        spark.sparkContext.setLogLevel("WARN")
        import spark.implicits._
        //        import org.apache.spark.sql.functions._
        val txtDF: DataFrame = spark.read
          .format("text")
          .option("inferSchema", true)
          //          .option("header",true)
          .option("path", "E:/test/text")
          .load()
        txtDF.printSchema()
        txtDF.show()
        txtDF.foreach(x=>{
            println(x)
            x
        })
    }

}
