package functionsOpt

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object AggregateFuncs {
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
    df.show()



  }
  def approx_count_distinct_demo(df:DataFrame,colName:String):Unit={
    //统计有多少个不重复的值,大概
    df.select(approx_count_distinct(colName)).show()
    //结果一样
//    print(df.select("order_number").distinct().count())

  }
  def collect_list_demo(df:DataFrame):Unit={
    //聚合成一个list，保留重复元素
    df.groupBy("user_id").agg(collect_list("order_hour_of_day")).printSchema()
  }
  def collect_set_demo(df:DataFrame):Unit={
    //聚合成一个set，去重
    df.groupBy("user_id").agg(collect_set("order_hour_of_day")).printSchema()
  }
  def corr_demo(df:DataFrame):Unit={
    //计算两列的皮尔逊相关系数，即线性相关性
    df.select(corr("order_id","order_number")).show()
  }
  def covar_pop_demo(df:DataFrame):Unit={
    //population covariance总体协方差
    df.select(covar_pop("order_id","order_number")).show()
  }
  def covar_samp_demo(df:DataFrame):Unit={
    //sample covariance样本协方差
    df.select(covar_samp("order_id","order_number")).show()
  }
  def kurtosis_demo(df:DataFrame):Unit={
    //计算峰度（峰态系数）
    df.select(kurtosis("order_hour_of_day")).show()
  }
  def skewness_demo(df:DataFrame):Unit={
    //计算偏度
    df.select(skewness("order_number")).show()
  }
  def stddev_demo(df:DataFrame):Unit={
    //样本标准差  alias for stddev_samp
    df.select(stddev("order_number")).show()
  }
  def stddev_pop_demo(df:DataFrame):Unit={
    //population standard deviation总体标准差
    df.select(stddev_pop("order_number")).show()
  }
  def stddev_samp_demo(df:DataFrame):Unit={
    //样本标准差  stddev_samp
    df.select(stddev_samp("order_number")).show()
  }
  def var_pop_demo(df:DataFrame):Unit={
    //总体方差
    df.select(var_pop("order_number")).show()
  }
  def variance_demo(df:DataFrame):Unit={
    //方差
    df.select(variance("order_number")).show()
  }

}
