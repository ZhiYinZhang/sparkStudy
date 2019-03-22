package functionsOpt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object DateFunc {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("aggregate")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val filepath = "E:\\test\\dawnbench\\train_results.csv"
    val df = spark.read
      .format("csv")
      .option("header",true)
      .option("inferSchema",true)
      .option("path",filepath)
      .load()

     df.show(truncate = false)


    //df.select(add_months($"timestamp",1)).show(truncate = false)
    //df.select(date_format($"timestamp","yyyy HH:mm:ss")).show(truncate=false)

    //day减去n天
    // df.select(date_sub(col("timestamp"),1)).show()

    //将时间戳时间 截断到指定格式  如"2018-12-02 12:25:00"  format="month"  return:"2018-12-01 00:00:00"
    //  df.select(date_trunc("month",col("timestamp"))).show(truncate = false)

    //df.select(datediff(current_timestamp(),col("timestamp"))).show(truncate = false)

    //df.select(dayofweek($"timestamp"),dayofmonth($"timestamp"),dayofyear($"timestamp")).show(truncate = false)


//    df.select(
//      quarter($"timestamp").as("quarter")
//    ).select(
//      when($"quarter"===1,"春季")
//      .when($"quarter"===2,"夏季")
//      .when($"quarter"===3,"秋季")
//      .when($"quarter"===4,"冬季")
//        .otherwise("null")
//      ).show(truncate = false)

//    df.select(year($"timestamp"),month($"timestamp"),hour($"timestamp"),minute($"timestamp"),second($"timestamp")).show()
    //将时间戳转换成seconds ，不传参用系统时间
//     df.select(unix_timestamp(col("timestamp"))).show(truncate = false)

//    df.groupBy(window($"timestamp","10 minute","5 minute")).mean("top5_correct").show(truncate = false)



      }
}
