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
    import spark.sql
    sc.setLogLevel("WARN")

    val df1=spark.createDataFrame(Seq(
      (1,10,3),
      (2,10,4),
      (4,10,6)
    )).toDF("a","b","c")


    //每行是从a开始到c结束的数组，step默认为1
//    val df2=df1.select(sequence($"a",$"b").as("arr"))
//    df2.show(false)
    //shuffle 将数组里面的值打乱
//    df2.select(shuffle($"arr")).show(false)

    //从1开始    根据索引取值
//    df2.select(element_at($"arr",1)).show()

    //concat 从2.4开始兼容数组
//    val ct=sql("select concat(array(1,2,3),array(4,5,6))")

    //reverse 从2.4兼容数组
//    sql("select reverse(array(1,2,3))").show()

    /**高阶函数   只能在sql中使用**/
    //数组中是否存在符合  条件的值
//    sql("select exists(array(1,2,3),x->x%2==0)").show()

    //过滤符合条件的元素
//    sql("select filter(array(1,2,3),x->x%2==0)").show()
    //将两个数组对应的元素传入到一个接受两个参数的函数中，如果其中一个较短，末尾补null
//    sql("select zip_with(array(1,2,3),array('a','b','c'),(x,y)->(y,x)) as tuple").show(false)
//    sql("select zip_with(array(1,2,3),array(3,1,4),(x,y)->x+y) as sum").show(false)

    //如果lambda函数有两个参数，第二个参数代表该元素的索引
    sql("select transform(array(1,2,3),x->x*2)").show()
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
