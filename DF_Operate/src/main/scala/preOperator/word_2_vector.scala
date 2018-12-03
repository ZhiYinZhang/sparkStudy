package preOperator

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

object word_2_vector {
    def main(args:Array[String]):Unit={
      val spark: SparkSession = SparkSession.builder()
        .appName("word2vec")
        .master("local[2]")
        .getOrCreate()
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

     val documentDF =  spark.createDataFrame(Seq(
        "Hi I heard about Spark".split(" "),
        "I wish Java could use case classes".split(" "),
        "Logistic regression models are neat".split(" ")
      ).map(Tuple1.apply)).toDF("text")
      documentDF.show(false)
      val word2vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(0)
      val model = word2vec.fit(documentDF)
      val result = model.transform(documentDF)

      result.show(false)

    }
}
