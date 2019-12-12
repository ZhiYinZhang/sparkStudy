package com.entrobus

import org.apache.spark.sql.SparkSession

object spark_binlog {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("spark binlog")
      .master("local[2]")
      .getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("WARN")


    val df = spark.readStream
      .format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource")
      .option("host","10.18.0.34")
      .option("port","3306")
      .option("userName","zhangzy")
      .option("password","123456")
      .option("databaseNamePattern","aistrong")
      .option("tableNamePattern","test1")
      .option("binlogIndex", "1")
      .option("binlogFileOffset", "49124")
      .option("bingLogNamePrefix","binlog")
      .load()

    df.writeStream.format("console").outputMode("append")
      .option("truncate",false)
      .start().awaitTermination()


    //    df.writeStream
    //    .format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource")
    //    .option("__path__","e://test//delta/test1/tmp/sysn/tables")
    //    .option("mode","Append")
    ////    .option("idCols","id")
    //    .option("duration","5")
    //    .option("syncType","binlog")
    //    .option("checkpointLocation","e://test//delta/test1/tmp/cpl-binlog2")
    //    .outputMode("append")
    //    .start("e://test//delta//test1/table")
    //    .awaitTermination()
  }
}
