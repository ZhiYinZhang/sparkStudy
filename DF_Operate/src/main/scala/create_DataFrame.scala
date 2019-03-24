import java.util

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder,encoderFor}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer

object create_DataFrame {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("garbage classfication")
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext
//    import spark.implicits._
    sc.setLogLevel("WARN")

    import spark.sql
//    sql("select reverse(array(4,5,6))").show()


//    create_4(spark)
  }

  case class Persion(name: String, age: Int)

  def create_0(spark: SparkSession): DataFrame = {
    //list里面放tuple
    val df = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "msg")
    df
  }

  def create_1(spark: SparkSession): DataFrame = {
    //一列，
    val df = spark.createDataFrame(Seq(
      Array("a", "b", "c"),
      Array("d", "e", "f"),
      Array("g", "h", "i")
    ).map(Tuple1.apply(_))).toDF("text")
    df
  }

  def create_2(spark: SparkSession) = {
    //list 里面放bean
    val list_bean = Array(
      Persion("Tom", 8),
      Persion("Joy", 35)
    )
    val df = spark.createDataFrame(list_bean)
  }

  def create_3(spark: SparkSession) = {
    //创建map
    import spark.implicits._
    val df = spark.createDataFrame(Seq(
      Map("a" -> 1, "b" -> 2),
      Map("b" -> 2, "c" -> 3),
      Map("c" -> 3, "d" -> 4)
    ).map(Tuple1.apply(_))).toDF("map")

    df.printSchema()
    val schema: StructType = df.schema
    println(schema)
    df.show()

    //to_json()
    val json = df.withColumn("json", to_json(df("map")))
    json.printSchema()
    json.show()
    Map[String, String]()

    // 和读取json文件时设置的schema一样
    val schema0 = new StructType().add("a", IntegerType).add("b", StringType)
      .add("c", IntegerType).add("d", StringType)
    //from_json()的options参数为spark.read.option().json()里面的option
    val map1 = json.withColumn("map1", from_json($"json", schema0))
    map1.printSchema()
    map1.show()
  }
  case class recommend(cust_id:String,word:String)
  def create_4(spark: SparkSession): Unit = {
    import spark.implicits._
    """
      |DataFrame的Row对象在schema里面是StructType类型
      |root
      | |-- key: integer (nullable = true)
      | |-- element: array (nullable = true)
      | |    |-- element: struct (containsNull = true)
      | |    |    |-- cust_id_int: integer (nullable = true)
      | |    |    |-- rating: string (nullable = true)
      |
    """.stripMargin
    val map = Map(1 -> "a", 2 -> "b", 3 -> "c", 4 -> "d", 5 -> "e",6->"f", 8 -> "h", 9 -> "i", 10 -> "j", 11 -> "k", 13 -> "m")
    val rows = new util.ArrayList[Row]()
    rows.add(Row(1, List(Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d"))))
    rows.add(Row(2, List(Row(8, "h"), Row(5, "e"), Row(10, "j"), Row(4, "d"))))
    rows.add(Row(3, List(Row(9, "i"), Row(13, "m"), Row(6, "f"), Row(11, "k"))))

    val schema = StructType(Seq(
      StructField("key", IntegerType),
      StructField("element", ArrayType(StructType(Seq(StructField("cust_id_int", IntegerType), StructField("word", StringType)))))
    ))
    //rows 是java.util.List 是一个接口，传入它的实现
    val df: DataFrame = spark.createDataFrame(rows, schema)

//    val df0=df.map(row => {
//      val old_rows: util.List[Row] = row.getList[Row](1)
//      var old_key = 0
//      var new_key:String = null
////      var new_row: GenericRowWithSchema = null
//      var new_row:recommend=null
////      val new_rows: ListBuffer[Row] = ListBuffer[Row]()
//      val new_rows:ListBuffer[recommend]=ListBuffer[recommend]()
//      val schema=StructType(Seq(StructField("cust_id",StringType),StructField("word",StringType)))
//      for (i <- 0 until old_rows.size()) {
//        old_key = old_rows.get(i).getInt(0)
//        new_key= map(old_key)
////        new_row=new GenericRowWithSchema(Array(new_key,old_rows.get(i).getString(1)),schema)
//        new_row=recommend(new_key,old_rows.get(i).getString(1))
//        new_rows.+=(new_row)
//      }
//      (row.getInt(0),new_rows)
//    })
//    df0.printSchema()
//    df0.show()
  }
}
