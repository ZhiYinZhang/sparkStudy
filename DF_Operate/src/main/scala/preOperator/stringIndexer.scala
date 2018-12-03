package preOperator

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

object stringIndexer {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("garbage classfication")
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val df = spark.createDataFrame(List(
      (0,"a"),
      (1,"b"),
      (2,"c"),
      (3,"a"),
      (4,"a"),
      (5,"c")
    )).toDF("id","category")
    //index的范围[0,numCategory(这一列的类别数)]
    //按照标签出现频率排序，出现最多的标签index为0
    //如果输入是数值型，先将数值映射到字符串
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("category_index")

    indexer.fit(df).transform(df).show(false)
  }
}
