import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.phoenix.spark._
import org.joda.time.DateTime
import org.apache.spark.sql.functions._

object spark_phoenix {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("read phoenix")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("warn")

//    val df=spark.range(10).withColumn("age",lit("100"))
//    df.saveToPhoenix(Map("table"->"test3","zkUrl"->"10.72.59.89:2181"))


    read_method1(spark).write.option("header",true).mode("overwrite").csv("e://test//retail")

  }




  def read_method1(spark:SparkSession):DataFrame={
    val df=spark.read.format("org.apache.phoenix.spark")
      .option("zkUrl","10.72.59.89:2181")
      .option("table","tobacco.retail")
      .load()
    return df
  }
 def read_method2(spark:SparkSession):DataFrame={
//   val conf=new Configuration()
//   conf.set("hbase.zookeeper.quorum","10.72.59.89:2181")
   val context = spark.sqlContext
   //字段区分大小写
   val df=context.phoenixTableAsDataFrame(
     "test3",List("AGE","NAME"),zkUrl = Option("10.72.59.89:2181"),predicate=Option("age='100'")
   )
   return df
 }


  def write_method1(df:DataFrame)={
    df.saveToPhoenix(Map("table"->"test3","zkUrl"->"10.72.59.89:2181"))
  }
  def write_method2(df:DataFrame)={
    df.write.format("org.apache.phoenix.spark")
      .mode("overwrite")//只能使用overwrite
      .option("table","test3")
      .option("zkUrl","10.72.59.89:2181")
      .save()
  }
}
