

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


    val df=spark.range(10)


    println(df.stat.bloomFilter("id",7,0.01).mightContainLong(1))

    print(df.stat.countMinSketch("id",1,1,1).addLong(1))
    df.show()
  }
}
