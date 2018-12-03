package preOperator

import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SparkSession

object n_gram {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("n-gram")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("id", "words")

    val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")

    ngram.transform(wordDataFrame).show(false)
  }
}
