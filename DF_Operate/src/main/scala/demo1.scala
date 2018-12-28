
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import scala.collection.mutable.ListBuffer
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
    sc.setLogLevel("WARN")

    val train_df = spark.read
      .format("csv")
      .option("header",true)
      .option("inferSchema",true)
      .option("path","E:\\test\\dawnbench\\train_results.csv")
      .load()
    val valid_df = spark.read
      .format("csv")
      .option("header",true)
      .option("inferSchema",true)
      .option("path","E:\\test\\dawnbench\\valid_results.csv")
      .load()
    train_df.printSchema()



// accuracy:87.07







  }

}
