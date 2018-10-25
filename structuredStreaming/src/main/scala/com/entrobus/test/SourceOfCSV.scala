package test

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SourceOfCSV {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .appName("csv")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val schema=new StructType().add("name","string")
      .add("age","integer")
      .add("job","string")

    val jsonDF: DataFrame = spark.readStream
      .schema(schema)
      .option("sep",";")
      .format("csv")
      .load("E:\\test\\csv")
//    val result: DataFrame = jsonDF.groupBy("age").count()
    val query:StreamingQuery=jsonDF.writeStream
      .outputMode("append")
      .format("csv")
            .option("checkpointLocation","e:\\test\\checkpoint")
            .option("path","E:\\test\\output3")
      .start()
    query.awaitTermination()
  }
}
