import java.util.Date
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object demo1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("demo")
      .master("local[3]")
      .config("spark.ui.port","36000")
//      .config("spark.speculation",true)
//      .config("spark.serializer","org.apache.spark.serializer.kryoSerializer")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val strlen = udf(strLen _)
    sc.setLogLevel("WARN")
    val df = spark.read
      .format("csv")
      .option("inferSchema",true)
      .option("header",true)
      .load("e://pythonProject//dataset//model01_train.csv")
    df.select(slice($"cnt",0,10)).show()


  }
  def strLen(num:Int):Boolean= {
    if (num > 10){
      return true
  }
    return false
  }

}
