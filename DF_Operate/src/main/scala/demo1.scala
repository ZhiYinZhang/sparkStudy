
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}

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
    sc.setLogLevel("WARN")



    val dataDF = spark.createDataFrame(Seq(
      new SimpleData("a",2.0),
      new SimpleData("b",1.0),
      new SimpleData("a",1.0)
    )).toDF("userId","rating")
    dataDF.dropDuplicates("userId").show()
    dataDF.distinct().show()

//    val doubles: Array[Double] = dataDF.stat.approxQuantile("value",Array(0,0.25,0.5,0.75,1),0.01)
//
//    val tuple: (Array[Double], Array[Long]) = dataDF.select("value").map{_.getAs[Double](0)}.rdd.histogram(6)



  }

}
