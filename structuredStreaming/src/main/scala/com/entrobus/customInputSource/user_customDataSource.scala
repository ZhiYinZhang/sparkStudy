package com.entrobus.customInputSource

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object user_customDataSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("customDataSource")
      .master("local[2]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    val schema: StructType = new StructType()
      .add("pid", IntegerType)
      .add("pname", StringType)
      .add("price", IntegerType)
      .add("category_id", StringType)


     val options = Map(
       "driverClass"->"com.mysql.jdbc.Driver",
       "jdbcUrl"->"jdbc:mysql://localhost:3306/entrobus",
       "user"->"root",
       "password"->"123456",
       "tableName"->"product"
     )

    val source = spark.readStream
      .format("org.apache.spark.sql.execution.streaming.mysqlSourceProvider")
      .options(options)
      .option("maxOffsetPerBatch",100)
      .schema(schema)
      .load()

    source.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate",false)
      .option("rowNum",100)
      .start()
      .awaitTermination()
  }
}
