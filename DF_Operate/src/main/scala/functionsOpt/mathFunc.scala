package functionsOpt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object mathFunc {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("aggregate")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val filepath = "E:\\test\\dawnbench\\train_results.csv"
    val df = spark.read
      .format("csv")
      .option("header",true)
      .option("inferSchema",true)
      .option("path",filepath)
      .load()

    df.show(truncate = false)
    // bin()二进制表达      bround()四舍五入  scale为小数点后几位上四舍五入，默认0
//    df.select(bin("epoch"),bround($"top5_accuracy",2)).show()

    //ceil()向上取整   conv()进制转换
//    df.select(ceil("top5_accuracy"),conv(col("batch"),10,2)).show()

    //rint()类似bround     round()保留小数点后几位
//    df.select(rint("top5_accuracy"),round($"loss",2)).show()

    df.select(hash($"batch")).show()
  }
}
