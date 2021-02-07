import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// case class SimpleData(name:String,value:Double)


object demo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("es")
      .master("local[2]")
      .config("es.nodes","192.168.35.164")
      .config("es.port","9200")
      .config("es.nodes.wan.only","true")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val test1=spark.readStream.format("rate").option("rowsPerSecond",10000).load()

    val res=test1.withColumn("timestamp",date_format($"timestamp","yyyy-MM-dd HH:mm:ss.SSS"))
      .withColumn("dt",date_format($"timestamp","yyyyMMdd"))
      .withColumn("message",$"value".cast("string"))



    val query=res.writeStream.format("es")
      .option("checkpointLocation","e://data//checkpoint")
      .outputMode("append")
      .format("es")
      .start("systemlog")

    query.awaitTermination()
//    test1.writeStream.format("console").start().awaitTermination()


  }
}
