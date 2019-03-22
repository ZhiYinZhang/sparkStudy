package functions_opt

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
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

    val df1=spark.createDataFrame(Seq(
      (1,10,3),
      (2,10,4),
      (4,10,6)
    )).toDF("a","b","c")
    //每行是从a开始到c结束的数组，step默认为1
    val df2=df1.select(sequence($"a",$"b").as("arr"))
    df2.show(false)
    //shuffle 将数组里面的值打乱
    df2.select(shuffle($"arr")).show(false)

    //从1开始    根据索引取值
    df2.select(element_at($"arr",1)).show()
  }

  def get_json_schema(spark:SparkSession):DataFrame={
    import spark.implicits._
    val df=spark.createDataFrame(Seq(
      Map("a"->1,"b"->2),
      Map("a"->2,"b"->3),
      Map("a"->3,"b"->4)
    ).map(Tuple1.apply(_))).toDF("map")
    //to_json 转成json字符串
    val json=df.withColumn("json",to_json($"map"))

    //获取json字符串的字段值 新建一列
    json.select(json_tuple($"json","a")).show()


    val sh: Column = schema_of_json(lit("""{"a":1,"b":[1,1,1],"c":{}}"""))

    json.select(schema_of_json("""{"j":1,"s":[1,2,3],"o":"json","n":{}}""")).show(false)
    json
  }
}
