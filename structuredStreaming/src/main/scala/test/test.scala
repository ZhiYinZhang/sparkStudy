package test

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.joda.time.DateTime

import org.apache.spark.sql.execution.streaming.customMysqlSourceProvider
object test {
  def main(args: Array[String]): Unit = {
       val spark=SparkSession.builder().appName("customMysqlSource").master("local[*]").getOrCreate()
     val schema=StructType(List(
       StructField("id",IntegerType),
       StructField("createTime",TimestampType),
       StructField("updateTime",TimestampType)
     ))

    val params = Map[String, String](
      "driverClass" -> "com.mysql.jdbc.Driver",
      "jdbcUrl" -> "jdbc:mysql://localhost:3306/test?useSSL=false&characterEncoding=utf-8",
      "user" -> "root",
      "password" -> "123456",
      "tableName" -> "test",
      "currentTime"->"2019-12-23 11:00:00"
    )


    val df=spark.readStream
      .format("org.apache.spark.sql.execution.streaming.customMysqlSourceProvider")
      .options(params)
      .schema(schema)
      .load()
    df.printSchema()
    df.writeStream.format("console")
      .outputMode("append")
      .option("truncate",false)
      .start()
      .awaitTermination()
  }
}
