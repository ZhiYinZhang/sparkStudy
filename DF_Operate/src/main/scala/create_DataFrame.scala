import java.util

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object create_DataFrame {
       def main(args:Array[String]):Unit={
         val spark: SparkSession = SparkSession.builder()
           .appName("garbage classfication")
           .master("local[1]")
           .getOrCreate()
         val sc = spark.sparkContext
         import spark.implicits._
         sc.setLogLevel("WARN")

//          val rows=Array(Row(1,"a"),Row(2,"b"),Row(3,"c"),Row(4,"d"))
         val rows = new util.ArrayList[Row]()
         rows.add(Row(1,"a"))
         rows.add(Row(2,"b"))
         rows.add(Row(3,"c"))
         RowEncoder
         val schema = StructType(Seq(StructField("key",IntegerType),StructField("value",StringType)))

         spark.createDataFrame(rows,schema).show()
         val schema1=new StructType()
             .add("item_id","int")
             .add("recommendations",ArrayType(StructType(Array(StructField("cust_id",IntegerType),StructField("rating",FloatType)))))
//         RowEncoder()

       }
  case class Persion(name:String,age:Int)

  def create_0(spark:SparkSession):DataFrame={
    //list里面放tuple
    val df = spark.createDataFrame(Seq(
      (0,Seq("I", "saw", "the", "red", "balloon")),
      (1,Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id","msg")
    df
  }
  def create_1(spark:SparkSession):DataFrame={
    //一列，
    val df = spark.createDataFrame(Seq(
      Array("a","b","c"),
      Array("d","e","f"),
      Array("g","h","i")
    ).map(Tuple1.apply(_))).toDF("text")
    df
  }
  def create_2(spark:SparkSession):DataFrame={
    //list 里面放bean
    val list_bean = Array(
      Persion("Tom",8),
      Persion("Joy",35)
    )
    val df = spark.createDataFrame(list_bean)
    df
  }
  def create_3(spark:SparkSession)={
    //创建map
    import spark.implicits._
    val df=spark.createDataFrame(Seq(
      Map("a"->1,"b"->2),
      Map("b"->2,"c"->3),
      Map("c"->3,"d"->4)
    ).map(Tuple1.apply(_))).toDF("map")

    df.printSchema()
    val schema: StructType = df.schema
    println(schema)
    df.show()

    //to_json()
    val json=df.withColumn("json",to_json(df("map")))
    json.printSchema()
    json.show()
    Map[String,String]()

    // 和读取json文件时设置的schema一样
    val schema0=new StructType().add("a",IntegerType).add("b",StringType)
      .add("c",IntegerType).add("d",StringType)
    //from_json()的options参数为spark.read.option().json()里面的option
    val map1=json.withColumn("map1",from_json($"json",schema0))
    map1.printSchema()
    map1.show()


  }
}
