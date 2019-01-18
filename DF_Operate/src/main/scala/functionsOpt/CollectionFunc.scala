package functionsOpt

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object CollectionFunc {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("aggregate")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val filepath = "e://test/market_basket"
    val df = spark.read
      .format("csv")
      .option("header",true)
      .option("inferSchema",true)
      .option("path",filepath)
      .load()
    //    1384617
    val orderDF: DataFrame = df.groupBy("user_id").agg(collect_list("order_id")).toDF("user_id","order_ids")
     orderDF.show(truncate=false)


    val df1 = spark.createDataFrame(List(
      (Map(1->"a"),Array(3,3,1),Array(1,2,1),1,3,1),
      (Map(2->"b"),Array(2,3,1),Array(3,2,1),0,4,2),
      (Map(3->"c"),Array(3,3,1),Array(1,2,1),0,3,1),
      (Map(4->"d"),Array(1,3,1),Array(1,1,1),-1,5,3)
    )).toDF("a","b","c","d","e","f")
     df1.show()


    // 与explode类似，只是多了一列（每个元素在每个数组中的索引）
//    df1.select(posexplode($"b")).show()

    //每行是从d开始到e结束的数组，step默认为1
//    df1.select(sequence($"d",$"e",$"f")).show(truncate = false)


  }

}
