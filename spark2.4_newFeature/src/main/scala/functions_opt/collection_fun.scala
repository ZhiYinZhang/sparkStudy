package functions_opt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object collection_fun {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("demo")
      .master("local[5]")
      .config("spark.ui.port","36000")
      //      .config("spark.speculation",true)
      //      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("WARN")

    val arr=spark.createDataFrame(Seq(
      Array(Array(1,1,1),Array(4,4,4)),
      Array(Array(2,2,2),Array(5,5,5))
    ).map(Tuple1.apply(_))).toDF("arr")


//    arr.select(flatten($"arr")).show()


    arr.select(schema_of_json("""{"j":1,"s":[1,2,3],"o":"json","n":{}}""")).show(false)


  }
}
