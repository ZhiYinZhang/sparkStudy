package preOperator

import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession

object index_to_string {
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
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    println(s"Transformed string column ${indexer.getInputCol} "+s"to indexed column '${indexer.getOutputCol}'")
    indexed.show(false)

    indexed.printSchema()

    val inputColSchema = indexed.schema(indexer.getOutputCol)
    println(inputColSchema)
    //StringIndexer输出列的元数据中存储了原来对应的string值
    println(s"StringIndexer will store labels in output column metadata: " +
      s"${Attribute.fromStructField(inputColSchema).toString}")

    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")
    val converted = converter.transform(indexed)
    converted.show(false)
  }
}
