package spark_nlp

import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import com.johnsnowlabs.nlp.annotators.{Normalizer, Tokenizer}
import com.johnsnowlabs.util.Benchmark
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SparkSession


object TokenizerWithNGram {
   def main(args:Array[String]):Unit={
     val spark: SparkSession = SparkSession.builder()
       .appName("tokenizerWithNGram")
       .master("local[4]")
//       .config("spark.driver.memory", "512m")
       .config("spark.kryoserializer.buffer.max", "100M")
       .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
       .getOrCreate()
     import spark.implicits._
     spark.sparkContext.setLogLevel("WARN")

     val document: DocumentAssembler = new DocumentAssembler()
       .setInputCol("text")
       .setOutputCol("document")

     val token: Tokenizer = new Tokenizer()
       .setInputCols("document")
       .setOutputCol("token")

     val normalizer: Normalizer = new Normalizer()
       .setInputCols("token")
       .setOutputCol("normal")

     val finisher: Finisher = new Finisher()
       .setInputCols("normal")

     val ngram: NGram = new NGram()
       .setN(3)
       .setInputCol("finished_normal")
       .setOutputCol("3-gram")

     val gramAssembler: DocumentAssembler = new DocumentAssembler()
       .setInputCol("3-gram")
       .setOutputCol("3-grams")

     val pipeline: Pipeline = new Pipeline()
       .setStages(Array(document, token, normalizer, finisher, ngram, gramAssembler))


     val testing = Seq(
       (1,"Google is a famous company"),
       (2,"Peter Parker is a super heroe")
     ).toDF("_id","text")

     val result = pipeline.fit(Seq.empty[String].toDS.toDF("text")).transform(testing)

     Benchmark.time("Time to convert and show"){
       result.show(truncate=false)
     }

   }
}
