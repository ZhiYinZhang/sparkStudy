package preOperator

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.SparkSession

object tokenizer {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("garbage classfication")
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val list_obj = List(
      Persion("a","Dear Spark Learner"),
      Persion("b","Thanks so much for attending the Spark Summit 2014"),
      Persion("c","Check out videos of talks from the summit at")
    )
    val df = spark.createDataFrame(list_obj)
    val tokenizer = new Tokenizer().setInputCol("message").setOutputCol("words")
    tokenizer.transform(df).show(false)
  }
}
