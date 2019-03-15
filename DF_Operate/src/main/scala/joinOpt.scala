import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id}

object joinOpt {
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

  val df0=spark.createDataFrame(Seq(
    (1,"a"),
    (2,"b"),
    (3,"c"),
    (4,""),
      (5,"e")
  )).toDF("id0","value0")
    val df1=spark.createDataFrame(Seq(
      (1,"a"),
      (2,"b"),
      (3,""),
      (4,"d"),
        (6,"f")
    )).toDF("id1","value1")

    //df0.join(df1,$"id0"===$"id1" && $"value0"===$"value1").show() 同下
    df0.join(df1,expr("id0=id1 and value0=value1")).show()

    df0.join(df1,$"id0"===$"id1","inner").show()
    df0.join(df1,"cross").show()
    df0.join(df1,$"id0"===$"id1","outer").show()
    df0.join(df1,$"id0"===$"id1","full").show()
    df0.join(df1,$"id0"===$"id1","full_outer").show()
    df0.join(df1,$"id0"===$"id1","left").show()
    df0.join(df1,$"id0"===$"id1","left_outer").show()
    df0.join(df1,$"id0"===$"id1","right").show()
    df0.join(df1,$"id0"===$"id1","right_outer").show()
    // 类似inner   但是只打印左表,不打印右表  只打印对于左表，右表中也存在的列
    df0.join(df1,$"id0"===$"id1","left_semi").show()
    //打印左表    打印左表存在，右表不存在的列
    df0.join(df1,$"id0"===$"id1","left_anti").show()
}
}
