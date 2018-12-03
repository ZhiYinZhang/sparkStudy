package preOperator

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession

object binarizer {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("binarizer")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val data = Array((0,0.1),(1,0.8),(2,0.2))
    val df = spark.createDataFrame(data).toDF("id","feature")

    val model = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarizer_feature")
      .setThreshold(0.5)
    model.transform(df).show(false)
  }
}
