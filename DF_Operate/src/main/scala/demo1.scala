
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import scala.collection.mutable.ListBuffer
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

    case class SimpleData(name:String,value:Double)

//    val dataDF = spark.createDataFrame(Seq(
//      SimpleData("a",2.0),
//      SimpleData("b",3.0)
//    )).toDF()

//    val doubles: Array[Double] = dataDF.stat.approxQuantile("value",Array(0,0.25,0.5,0.75,1),0.01)
//
//    val tuple: (Array[Double], Array[Long]) = dataDF.select("value").map{_.getAs[Double](0)}.rdd.histogram(6)



  }

}
