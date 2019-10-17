import java.sql.Time
import java.text.SimpleDateFormat
import java.sql.Date

import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import shaded.parquet.org.codehaus.jackson.map.ext.JodaDeserializers.DateTimeDeserializer

import scala.util.Random

object columnOpt {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("barrierDemo1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
//   win_fun2(spark)

     extract_for_struct(spark)


  }

  def extract_for_struct(spark:SparkSession):Unit={
    /**
      *StructType StructType(StructType) ArrayType(StructType):
      * col("c1.id")
      * col("c1")("id")
      * expr("c1['id']")  #要加引号
      * selectExpr("c1['id']") # select里面不能用 c1[id]
      *
      * ArrayType还可以用索引
      * expr("arr[1]")
      * 如果ArrayType里面是StructType,还可以:
      *   expr("arr[1].id")
      */
    //从结构化类型的列中提取值
    //构造StructType的列
    val df=spark.range(10)
      .withColumn("value",randn())
      .withColumn("struct",struct("id","value"))

    //提取  直接点
//    df.select(col("struct.id"),col("struct.value")).show()
//    df.select(col("struct")("value")).show()
//    df.selectExpr("struct['id']").show()
//    df.select(expr("struct['id']")).show()


    //StructType的列里面嵌套StructType
    val df1=df.withColumn("value1",rand())
      .withColumn("struct1",struct("struct","value1"))
    df1.printSchema()
    df1.select("struct1.struct.value","struct1.value1").show()
    df1.selectExpr("struct['id']").show()


    //ArrayType(StructType)
//    val df2=df.withColumn("struct1",struct("id","value"))
//      .withColumn("arr",array("struct","struct1"))
//    df2.printSchema()
//    df2.selectExpr("arr.id","arr.value","arr[1].id").show(false)


    //ArrayType(IntegerType)  selectExpr可以  select不能
//    df.withColumn("arr",array(lit(4),lit(2)))
//      .select(expr("arr[1]")).show()
  }

  def when_fun(spark:SparkSession):Unit={
    val df=spark.range(5000)
    // sql中的case when
    var when_df=df.select(
      when(col("id")<1000,0)
        .when(col("id")<2000,1)
        .when(col("id")<3000,2)
        .otherwise(3).as("when")
    ).groupBy("when").count()

    when_df.select(col("id").between(1,100)).show()
  }

  def win_fun(df:DataFrame):DataFrame={

    //窗口函数
    val win: WindowSpec = Window.partitionBy("mnth")
      .orderBy(col("mnth").asc)
      .rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

    df.select(col("mnth"),sum("instant").over(win))

  }

  def win_fun2(spark:SparkSession):Unit={
    import spark.implicits._
    //计算 分组后 同一列 后一行的值 减 前一行的值

    val win=Window.orderBy("id")
    val df=spark.range(20).withColumn("date",lit("0"))


    df.map(x=>{
        val random = new Random()
        val dt=new DateTime()
        val date=dt.plusDays(random.nextInt(22)).toString("yyyy-MM-dd")

        (x.getLong(0),date)
      })
      .toDF("id","date")
      .withColumn("date_lead",lead(col("date").cast("date"),1).over(win))
      .show()
  }
}
