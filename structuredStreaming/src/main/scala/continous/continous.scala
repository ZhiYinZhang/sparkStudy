package continous

import java.sql.Timestamp
import java.util.Properties

import generateData.{deleteDir, readProperties}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.Trigger

object continous {
  def main(args:Array[String]):Unit={

    val prop: Properties = readProperties("D:\\IdeaProjects\\demo\\structuredStreaming\\src\\test.properties")
    val checkpointLocation=prop.getProperty("checkpointLocation")
    val kafkaHost: String = prop.getProperty("kafkaHost")
    val topic: String = prop.getProperty("topic")
    deleteDir(checkpointLocation)
    if(checkpointLocation==null || kafkaHost==null || topic==null){
      throw new NoSuchElementException("参数名称错误！！！需要如下参数:checkpointLocation,kafkaHost,topic")
    }

    val rows=1000

    val spark=SparkSession.builder()
      .master("local[5]")
      .appName("continuous")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val rateDF: DataFrame = spark.readStream
      .format("rate")
      .option("rowsPerSecond",rows)//rows=10000
      .load()
    rateDF.printSchema()
    val value: Dataset[(Timestamp, Long)] = rateDF.as[(Timestamp,Long)]
    val filter: Dataset[(Timestamp, Long)] = value.filter("value%500=0")
    val result: Dataset[Tuple1[String]] = filter.map(x => {
      val tuple = new Tuple1(x._1 + "\t" + x._2)
      tuple
    })

    /*val resultDS: Dataset[Row] = rateDF.map(x => {
      println(x.get(0) + " " + x.get(1))
      x
    })*/

    result.toDF("value")
      .selectExpr("cast(value as string)")
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("checkpointLocation",checkpointLocation)
      .option("kafka.bootstrap.servers",kafkaHost)
      .option("topic",topic)
      .trigger(Trigger.Continuous(1000))
      .start()
      .awaitTermination()

  }
}
