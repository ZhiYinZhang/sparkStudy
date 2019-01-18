
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

    val df1: DataFrame = spark.range(0,10000000).toDF("col1")

    val start = System.currentTimeMillis()

    val df2: DataFrame = df1.withColumn("partitionId",spark_partition_id())
    df2.groupBy("partitionId").count().show()

    val end = System.currentTimeMillis()

    print(end-start)


    



  }

}
