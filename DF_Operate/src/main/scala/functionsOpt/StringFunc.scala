package functionsOpt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object StringFunc {
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
      (Map(1->"a"),Array(3,3,1),Array(1,2,1),1,34545454,"ABC"),
      (Map(2->"b"),Array(2,3,1),Array(3,2,1),0,4121211,"abca11"),
      (Map(3->"c"),Array(3,3,1),Array(1,2,1),0,311111,"求其上者"),
      (Map(4->"d"),Array(1,3,1),Array(1,1,1),-1,11111,"123")
    )).toDF("a","b","c","d","e","f")
    df1.show()
   //求字符串第一个字符的ascii码
//   df1.select(ascii($"f")).show()

//    df1.select(base64(bin("f"))).show()
   //将多个字符串列按照指定的分隔符连接成一列
//    df1.select(concat_ws("===",$"f",$"f")).show()

//    df1.select(
//      encode(decode($"f","US-ASCII"),"US-ASCII")
//      ,decode($"f","ISO-8859-1")
//      ,decode($"f","UTF-8")
//      ,decode($"f","UTF-16BE")
//      ,decode($"f","UTF-16LE")
//      ,decode($"f","UTF-16")
//    ).show()

// format like '#,###,###.##' d为多少为小数点
//    df1.select(format_number($"e",d=5)).show()

//    df1.select(format_string("aaa",$"f")).show()

   //每个单词的第一个字母转成大写
//   df1.select(initcap($"f")).show()
   //instr()substring所在位置     locate()类似
//    df1.select(instr($"f","a"),length($"f")).show()

//    df1.select(levenshtein($"f",$"f"+"111")).show()

//字符串左侧用pad字符串填充至len长度，如果len小于字符串长度，则将字符串截断成len长度，保留左侧
//    df1.select(lpad($"f",10,"c")).show()

//    df1.select(soundex($"f")).show()


//    df1.select(translate($"f","者","乎"),regexp_replace($"f",".{1,2}者","乎")).show()

    //以delim为分隔符分隔，count为返回以第二个分隔符分隔得到的字符串
//    df1.select(substring_index($"f","a",2)).show()


  }
}
