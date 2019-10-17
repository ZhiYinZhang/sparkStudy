package functions_opt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object non_aggregate {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("aggregate")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._


    val df=spark.createDataFrame(Seq(
      (Array(1,2,3,4,5,6),Array(2,3,4,5,6,7))
    )).toDF("arr0","arr1")

    df.select(map_from_arrays($"arr0",$"arr1")).show()



    val df0 = spark.range(10).withColumn("value0",lit(0))
    val df1 = spark.range(5,15).withColumn("value1",lit(1))

    val df2 = df0.join(df1,Seq("id"),"outer")


    df2.select((for(c <- df2.columns)yield (lit(1)-count(c)/count("*")).as(c+"_null")):_*).show()
  }
}
