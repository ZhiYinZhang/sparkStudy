
import org.apache.spark.api.java.JavaRDD
//import org.apache.spark.sql.functions.{monotonically_increasing_id, udf,col}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.types._
//import java.lang.Math._

import scala.collection.mutable.ListBuffer
case class SimpleData(name:String,value:Double)
object demo1 {
  def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .appName("demo")
          .master("local[5]")
          .config("spark.ui.port","36000")
    //      .config("spark.speculation",true)
    //      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()
        val sc = spark.sparkContext
        import spark.implicits._
//        sc.setLogLevel("WARN")

        val df = spark.read.option("header",true).option("inferSchema",true).csv("E:\\test\\city_map\\example2018-12-05-12-02-23去重结果.csv")

       val  df_id=df.withColumn("cust_id",monotonically_increasing_id())

        df_id.printSchema()
        val array: Array[Dataset[Row]] = df_id.randomSplit(Array(0.5,0.5))
        val df0=array(0)
        val df1=array(1).withColumnRenamed("wgs_lng","wgs_lng1")
          .withColumnRenamed("wgs_lat","wgs_lat1")
//
        val df_cr: DataFrame = df0.crossJoin(df1)
        df_cr.select(col("*"),mul_col($"wgs_lng",$"wgs_lat",$"wgs_lng1",$"wgs_lat1").name("haversine")).show()

        println(df_cr.count())

  }
def mul_col(lng1:Column,lat1:Column,lng2:Column,lat2:Column):Column={
      val radius = 6378137
      val radLng1=radians(lng1)
      val radLat1=radians(lat1)
      val radLng2=radians(lng2)
      val radLat2=radians(lat2)

  val result = asin(sqrt(pow(sin((radLat1-radLat2)/2.0),2)+ cos(radLat1)*cos(radLat2)*pow(sin((radLng1-radLng2)/2.0),2)))*2.0*radius
   result
}
//  private val mul_udf: UserDefinedFunction = udf((lng1:Double,lat1:Double,lng2:Double,lat2:Double)=>{
//    val radius = 6378137
//    val radLng1=Math.toRadians(lng1)
//    val radLat1=Math.toRadians(lat1)
//    val radLng2=Math.toRadians(lng2)
//    val radLat2=Math.toRadians(lat2)
//
//    val result=2.0 * asin(sqrt(
//      pow(sin((radLat1 - radLat2) / 2.0), 2) +
//        cos(radLat1) * cos(radLat2) * pow(sin((radLng1 - radLng2) / 2.0), 2))
//    ) * radius
//    result
//  },DoubleType)

}
