package functionsOpt

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object CollectionFunc {
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
    val orderDF: DataFrame = df.groupBy("user_id").agg(collect_list("order_id")).toDF("user_id","order_ids")
     orderDF.show(truncate=false)


    val df1 = spark.createDataFrame(List(
      (Map(1->"a"),Array(3,3,1),Array(1,2,1),1,3,1),
      (Map(2->"b"),Array(2,3,1),Array(3,2,1),0,4,2),
      (Map(3->"c"),Array(3,3,1),Array(1,2,1),0,3,1),
      (Map(4->"d"),Array(1,3,1),Array(1,1,1),-1,5,3)
    )).toDF("a","b","c","d","e","f")
     df1.show()


    // 与explode(将数组中每一个值作为一行)类似，只是多了一列（每个元素在每个数组中的索引）
    df1.select(posexplode($"b")).show()
    // typedLit 支持 List，Map等类型
     df1.select(lit("a"),typedLit(List(1,2,3,4))).show()
  }

  def json_opt(spark:SparkSession){
    import spark.implicits._
    val ds=spark.createDataFrame(Seq(
      Map("a"->1,"b"->2),
      Map("b"->2,"c"->3),
      Map("c"->3,"d"->4)
    ).map(Tuple1.apply(_))).toDF("map")

    ds.printSchema()
    val schema: StructType = ds.schema
    println(schema)
    ds.show()

    val json=ds.withColumn("json",to_json(ds("map")))
    json.printSchema()
    json.show()
    Map[String,String]()

    val schema0=new StructType().add("a",IntegerType).add("b",StringType)
      .add("c",IntegerType).add("d",StringType)
    //
    val map1=json.withColumn("map1",from_json($"json",schema0))
    map1.printSchema()
    map1.show()
  }
}
