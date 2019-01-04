package functionsOpt

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object non_aggregateFunc {
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
//    val orderDF: DataFrame = df.groupBy("user_id").agg(collect_list("order_id")).toDF("user_id","order_ids")
//    orderDF.show(truncate=false)


    val df1 = spark.createDataFrame(List(
      (Map(1->"a"),Array(3,3,1),Array(1,2,1),1,3,1),
      (Map(2->"b"),Array(2,3,1),Array(3,2,1),0,4,2),
      (Map(3->"c"),Array(3,3,1),Array(1,2,1),0,3,1),
      (Map(4->"d"),Array(1,3,1),Array(1,1,1),-1,5,3)
    )).toDF("a","b","c","d","e","f")
    df1.show()

//    df1.select($"*",array("f")).show()

    // bitwiseNOT()按位取反
//    df1.select($"*",bitwiseNOT($"f")).show()

    //expr()表达式类似selectExpr()   类似sql：select sum(f) from df1.
//    df1.select(expr("sum(f)")).show()

    //  greatest()返回所选列的组成的每一行的最大值    least 返回最小值，与greatest()对应
//    df1.select(greatest("f","e"),input_file_name()).show()

      //lit生成一列，以相同的值
    //rand   从[0.0,1.0]独立且相同分布样本的随机列。
    //randn  从标准正态分布生成具有独立且相同分布样本的列
//    df1.select(
//      lit(1).as("lit")
//      ,rand()
//      ,rand(10)
//      ,randn()
//      ,randn(10)
//    ).show()

//     print(spark.sparkContext.defaultParallelism)
//     df.select(spark_partition_id().as("part")).distinct().show()
     //monotonically_increasing_id()单调递增唯一 不连续
//     df.select(monotonically_increasing_id().as("index")).select(max("index"),count("index")).show()
       //将第一列作为key，第二列作为value
//    df1.select(map($"e",$"f")).show()


//    df1.sort(asc("f")).show()
//    df1.sort(asc_nulls_first("f")).show()
//    df1.sort(asc_nulls_last("f")).show()

  }
}
