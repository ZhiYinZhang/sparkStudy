package preOperator

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession

object stopWords {
      def main(args:Array[String]):Unit={
        val spark: SparkSession = SparkSession.builder()
          .appName("garbage classfication")
          .master("local[1]")
          .getOrCreate()
        val sc = spark.sparkContext
        sc.setLogLevel("WARN")

        //停用词
              val df1 = spark.createDataFrame(List(
                (0, Seq("I", "saw", "the", "red", "balloon")),
                (1, Seq("Mary", "had", "a", "little", "lamb"))
              )).toDF("id","raw")
              val remover = new StopWordsRemover()
                .setInputCol("raw")
                .setOutputCol("filtered")
              remover.transform(df1).show(false)
      }
}
