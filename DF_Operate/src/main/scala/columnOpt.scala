import java.sql.Time
import java.text.SimpleDateFormat
import java.sql.Date

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, current_date, decode, lit, sum, when,lead}
import org.joda.time.DateTime
import shaded.parquet.org.codehaus.jackson.map.ext.JodaDeserializers.DateTimeDeserializer

import scala.util.Random

object columnOpt {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("barrierDemo1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
//
   win_fun2(spark)

  }
  def when_fun(spark:SparkSession):Unit={
    val df=spark.range(5000)
    // sql中的case when
    var when_df=df.select(
      when(col("id")<1000,0)
        .when(col("id")<2000,1)
        .when(col("id")<3000,2)
        .otherwise(3).as("when")
    ).groupBy("when").count()

    when_df.select(col("id").between(1,100)).show()
  }

  def win_fun(df:DataFrame):DataFrame={

    //窗口函数
    val win: WindowSpec = Window.partitionBy("mnth")
      .orderBy(col("mnth").asc)
      .rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

    df.select(col("mnth"),sum("instant").over(win))

  }

  def win_fun2(spark:SparkSession):Unit={
    import spark.implicits._
    //计算 分组后 同一列 后一行的值 减 前一行的值

    val win=Window.orderBy("id")
    val df=spark.range(20).withColumn("date",lit("0"))


    df.map(x=>{
        val random = new Random()
        val dt=new DateTime()
        val date=dt.plusDays(random.nextInt(22)).toString("yyyy-MM-dd")

        (x.getLong(0),date)
      })
      .toDF("id","date")
      .withColumn("date_lead",lead(col("date").cast("date"),1).over(win))
      .show()
  }
}
