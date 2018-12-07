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

    sc.setLogLevel("WARN")

    val df1 = spark.createDataFrame(Seq(
      Array(1,2,4,4),
      Array(5,9,7,8),
      Array(8,9,6,11)
    ).map(Tuple1.apply(_))
    ) .toDF("test")
    df1.select(array_sort(col("test"))).show()
    
    df1.printSchema()
    df1.show()
  }

}
