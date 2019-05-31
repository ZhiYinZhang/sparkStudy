

import java.lang

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.util.Random
import scala.collection.mutable.Map

case class SimpleData(name:String,value:Double)
object demo1 {
  def main(args: Array[String]): Unit = {
      val spark=SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()
      import spark.implicits._
      spark.sparkContext.setLogLevel("warn")


    val df = spark.createDataFrame(Seq((1, 4), (1, 5), (2, 4), (2, 4), (2, 6), (3, 5), (3, 6)))
                      .toDF("key", "value")
    df.show()
    df.stat.crosstab("key","value").show()

  }
}
