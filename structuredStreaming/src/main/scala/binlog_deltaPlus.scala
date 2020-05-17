import org.apache.spark.sql.functions.{col, from_json,schema_of_json}
import org.apache.spark.sql.{DataFrame, SparkSession}

object binlog_deltaPlus {
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
      .option("tableNamePattern",".*")
      .option("binlogIndex", "1")
      .option("binlogFileOffset", "18107629")
      .option("bingLogNamePrefix","binlog")
      .load()


//    val df1=parse(df).repartitionByRange(10,col("tableName"))
    val df1=df.repartitionByRange(10,col("value"))

       df1.printSchema()

        df1.writeStream
        .format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource")
        .option("path","e://test//delta/test1/aistrong/test1")
        .option("mode","Append")
        .option("idCols","value")
        .option("duration","5")
        .option("syncType","binlog")
        .option("checkpointLocation","e://test//delta/tmp/cpl-binlog2")
        .outputMode("append")
        .start()
        .awaitTermination()
  }
  def parse(df:DataFrame):DataFrame={
    //根据value
    val schema=schema_of_json("""{"type":"insert","timestamp":1576114094000,"databaseName":"aistrong","tableName":"test1","schema":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"a\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"b\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","rows":[{"id":5,"a":1,"b":1}]}""")

    val df1=df.withColumn("value",from_json(col("value"),schema))
      .selectExpr("value.*")
    df1
  }

}
