import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.element_at
object state_Info {
    def main(args:Array[String]):Unit={
      val spark: SparkSession = SparkSession.builder()
        .appName("foreachBatch")
        .master("local[2]")
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      import spark.implicits._

      val df1: DataFrame = spark.readStream
        .format("rate")
        .option("rowsPerSecond",5)
        .option("rampUpTime",3) //初始化好了，再等多久产生数据
        .load()

      val df2= df1.selectExpr("data.id","data.runId")

      df2.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate",true)
        .option("numRows",10)
        .start()
        .awaitTermination()
  }
}
