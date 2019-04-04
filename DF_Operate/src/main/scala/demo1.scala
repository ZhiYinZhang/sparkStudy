

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

     df.printSchema()
    df.show(false)
    df.map{case Row(item_id:Int,cust_id:Int,qty_ord:Int,born_date:Int)=>(item_id)}

    //      val data_dir=param("data_dir").toString
    //      val userCol=param("userCol").toString
    //      val itemCol=param("itemCol").toString
    //      val ratingCol=param("ratingCol").toString
    //      val top=param.getOrElse("top",10).toString.toInt



//    df=df.withColumn("born_date",to_date($"born_date".cast("string"),"yyyyMMdd"))
//    .withColumn("current_date",date_sub(current_date(),78))
//    .withColumn("date_diff",datediff(col("current_date"),col("born_date")))

//    val day_7=df.where(df("date_diff")<7).groupBy("item_id","cust_id").agg(sum("qty_ord").alias("qty_ord")).withColumn("lately",lit(7))
    ////    val day_30=df.where(df("date_diff")<30).groupBy("item_id","cust_id").agg(sum("qty_ord").alias("qty_ord")).withColumn("lately",lit(30))
    ////    val day_180=df.where(df("date_diff")<180).groupBy("item_id","cust_id").agg(sum("qty_ord").alias("qty_ord")).withColumn("lately",lit(180))
    ////
    ////    var df1:DataFrame=day_7.unionByName(day_30).unionByName(day_180)
    ////
    ////    df1=df1.orderBy("item_id","cust_id","lately")


//    val df0=df.groupBy("item_id","cust_id").count().orderBy("item_id","cust_id")
//    df0.show()
//    df0.explain()


//    Thread.sleep(1000*60*10)


    //    df0.selectExpr(List("item_id","cust_id"):_*).show()

//    df.select(countDistinct("item_id","cust_id")).show()
//    df.select(countDistinct("item_id"),countDistinct("cust_id")).show()


  }
}
