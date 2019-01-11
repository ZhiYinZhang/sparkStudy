import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
/*
spark shuffle 的几种排序算法 及 压缩算法
Spark的Shuffling中有两个重要的压缩参数。
         spark.shuffle.compress true---是否将会将shuffle中outputs的过程进行压缩。
         将spark.io.compression.codec编码器设置为压缩数据，默认是true.同时，
通过spark.shuffle.manager 来设置shuffle时的排序算法，
         hash（用hash会快一点，我不需要排序啊~）
         sort
         tungsten-sort
*/

object shuffle_sort_alg {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("shuffleSortALG")
      .master("local[5]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._

    val df1 = spark.range(0,10000000).toDF("col1")


    val df2 = df1.map(x=>{
      val value = x.getLong(0)
      (value,value.hashCode()%5)
    }).toDF("col1","col2")

      val start = System.currentTimeMillis()
      val df3: DataFrame = df2.groupBy("col2").agg(max("col1"),min("col1"),avg("col1"))
      df3.collect()

      val end = System.currentTimeMillis()

      println(end-start,"ms")


    print(spark.conf.get("spark.shuffle.manager"))


  }
}
