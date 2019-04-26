

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
        val spark: SparkSession = SparkSession.builder()
          .appName("demo")
          .master("local[3]")
          .config("spark.ui.port","36000")
//          .config("SPARK_LOCAL_DIRS","e://test//temp")
    //      .config("spark.speculation",true)
    //      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()
        val sc = spark.sparkContext
        import spark.implicits._
        sc.setLogLevel("WARN")


    val path="e://test/temp//order1.csv"
    var df:DataFrame=spark.read
        .option("header",true)
      .option("inferSchema",true)
      .csv(path)
    val bd = sc.broadcast(df)
    df=bd.value

     df.show(false)

    df.groupBy("cust_id").agg(sum("qty_ord").alias("qty_ord")).show()

    Thread.sleep(1000*60*5)


  }
}
