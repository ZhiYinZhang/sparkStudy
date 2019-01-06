package functionsOpt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
object WinFunc {
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
      (6,"a"),
      (2,"b"),
      (3,"c"),
      (3,"d"),
      (4,"e"),
      (5,"f")
    )).toDF("id","c")
    df1.show()

   //rowsBetween 按照当前行的相对位置
//    val win1 = Window.partitionBy("c").orderBy("id").rowsBetween(0,1)
    //rangeBetween(必须要orderBy) 按照orderBy排序后的位置  -1表示前一名，1表示后一名
    //在排名没有并列的情况下，与rowsBetween一样，
    //排名并列，有数据(2,1,2)，排序后(1,2,2),进行sum，在(currentRow,1)窗口下，第一：(1) 第二：(2,2)
    // 结果为(5,4,4),第一个窗口的值5，表示当前名次和下一名次的值相加，即第一+第二=1+（2+2）
    //第二个窗口的值4，第二+第三=（2+2）+0
    //第三个窗口的值4，第二+第三=（2+2）+0
//    val win2 = Window.orderBy("id").rangeBetween(0,1)
//      df1.select(sum("id").over(win1)).show()
//     df1.select(sum("id").over(win2)).show()

    //计算  (当前排名到第一名有多少行)/total
    val win = Window.orderBy("id")
//    df1.select($"*",cume_dist().over(win)).show()

     //deng_rank()  并列的排名相同，下一名序号连续
//    df1.select($"*",dense_rank().over(win),rank().over(win),row_number().over(win)).show()

    //分成4份，返回partition id
//    df1.select(ntile(4).over(win)).show()

    //lag()   当前行向前偏移n的值,即落后n的值    1表示当前行的前一行的值  如果为null，可以使用default
    //lead()  当前行向后偏移n的值，即超前n的值
    //不过如果offset为负数的话，lag和lead反过来
    df1.select($"*",lag("id",1).over(win),lead("id",1).over(win)).show()


  }
}
