package test

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SourceOfJson {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .appName("json")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val schema=new StructType().add("time","integer")
      .add("action","string")

    val jsonDF: DataFrame = spark.readStream
      .schema(schema)
      .format("json")
      .load("E:\\test\\json")
    val result: DataFrame = jsonDF.groupBy("action").count()

    jsonDF.createOrReplaceTempView("json")
    val frame: DataFrame = spark.sql("select * from json sort by 'time'")


    val query:StreamingQuery=frame.writeStream
        .outputMode("complete")
      .format("console")
//      .option("checkpointLocation","e:\\test\\checkpoint")
//      .option("path","E:\\test\\output3")
      .start()
    query.awaitTermination()
  }
}
