package preOperator



import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.sql.SparkSession

object One_Hot {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("garbage classfication")
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c"),
      (6,"d")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("categoryIndex","id"))
      .setOutputCols(Array("categoryVec"))
//        .setDropLast(false)
    val encoded = encoder.fit(indexed).transform(indexed)
    encoded.printSchema()
    encoded.show(false)

  }
}
