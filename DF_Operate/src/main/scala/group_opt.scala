import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object group_opt {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("group operator")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("warn")
    pivot(spark)


  }

  def pivot(spark:SparkSession)={
   // pivot 透视
    val data=Seq(
      ("2019-01-01",1),
      ("2019-01-15",2),
      ("2019-02-01",2),
      ("2019-02-15",4),
      ("2018-01-01",1),
      ("2018-01-15",2),
      ("2018-02-01",2),
      ("2018-02-15",4)
    )

    val df=spark.createDataFrame(data).toDF("date","value")
    val df1=df.withColumn("year",year(col("date")))
      .withColumn("month",month(col("date")))

    //统计
    df1.groupBy("year","month")
      .agg(sum("value"),avg("value")).show()
    //也是按year month分组  但是会将month的值作为列,每列的值是统计值
    df1.groupBy("year").pivot("month")
      .agg(sum("value"),avg("value")).show()


  }
}
