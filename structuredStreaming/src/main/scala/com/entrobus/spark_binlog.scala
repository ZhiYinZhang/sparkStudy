package com.entrobus

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object spark_binlog {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("spark binlog")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val sc=spark.sparkContext
    sc.setLogLevel("WARN")

 //DataFrame只有一个字段 value，里面是一个json字符串
    /**
      * 数据
    {
      "type": "insert",  //事件类型，insert，update，delete
      "timestamp": 1576114094000,
      "databaseName": "aistrong",   //数据库
      "tableName": "test1",       //表
      "schema": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"a\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"b\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
      "rows": [{
        "id": 5,
        "a": 1,
        "b": 1
      }]
    }
      */
    val df = spark.readStream
      .format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource")
      .option("host","10.18.0.34")
      .option("port","3306")
      .option("userName","zhangzy")
      .option("password","123456")
      .option("databaseNamePattern","aistrong")
      .option("tableNamePattern","test1")
      .option("binlogIndex", "1")
      .option("binlogFileOffset", "1343746")
      .option("bingLogNamePrefix","binlog")
      .load()

    val df1=parse(df)

    df1.repartitionByRange(10,col("id"))


    val path="e://test//delta//test"
    val deltaTable=DeltaTable.forPath(spark,path)

    def upsertToDelta(batchDF:DataFrame,batchId:Long): Unit ={
      println(batchId)
      //消费binlog，除了insert/update/delete事件外，还有其他mysql系统事件
      // 这些事件里面没有数据，但是还是会有batch，所以会造成delta很多空文件
      //可以判断DataFrame的数据量
      batchDF.persist(StorageLevel.DISK_ONLY)
      if(batchDF.count()>0){
        deltaTable.as("t")
          .merge(batchDF.as("s"),"t.id=s.id")
          .whenMatched("type='update'").updateAll()
          .whenMatched("type='delete'").delete()
          .whenNotMatched("type='insert'").insertAll()
          .execute()
      }

    }

    df1.writeStream
      .format("delta")
      .foreachBatch(upsertToDelta _)
      .outputMode("update")
      .start()
      .awaitTermination()
//
//    df1.writeStream.format("console").outputMode("append")
//      .option("truncate",false)
//      .start().awaitTermination()
  }
  def parse(df:DataFrame):DataFrame={
    //根据value
    val schema=schema_of_json("""{"type":"insert","timestamp":1576114094000,"databaseName":"aistrong","tableName":"test1","schema":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"a\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"b\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","rows":[{"id":5,"a":1,"b":1}]}""")

    val df1=df.withColumn("value",from_json(col("value"),schema))
      .selectExpr("value.type","value.databaseName as database","value.tableName as table","value.rows[0] as rows")
      .selectExpr("*","rows.a","rows.b","rows.id")
    df1
  }


}
