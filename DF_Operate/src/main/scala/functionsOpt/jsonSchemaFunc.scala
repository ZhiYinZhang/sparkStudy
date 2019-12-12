package functionsOpt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object jsonSchemaFunc {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("jsonSchema")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._


    val data=Seq(
      (1,2,3),
      (1,2,3),
      (1,2,3)
    )

    val df=spark.createDataFrame(data).toDF("a","b","c")

    val df1=df.withColumn("st1",struct("a","b"))
      .withColumn("st2",struct("st1","c"))

    val schema = df1.schema

    println(schema.toDDL)
    println(schema.json)
    //`st1` STRUCT<`a`: INT, `b`: INT>
    println(schema.apply("st1").toDDL)
   //{"type":"struct","fields":[{"name":"a","type":"integer","nullable":false,"metadata":{}},{"name":"b","type":"integer","nullable":false,"metadata":{}}]}
    println(schema.apply("st1").dataType.json)


//将StructType或ArrayType类型的列 转成json
    val df2=df1.withColumn("json",to_json($"st1"))

    //获取json字符串的schema
//    val schema_json = schema_of_json("""{"a":0,"b":2}""")
//将json字符串列转成StructType
//    df2.select(from_json($"json",schema_json)).show()

    //将schema转成DDL字符串
    val ddl = schema.apply("st1").toDDL

    //使用DDL格式的string解析 json字符串;  options：可以接收和json数据源一样的参数，spark.read.options().json()
//    df2.select(from_json(col("json"),"STRUCT<`a`:INT,`b`:INT>",Map(""->""))).show()

    //
//    val json_schema=schema.apply("st1").dataType.json
//    df2.select(from_json($"json",json_schema,Map(""->""))).show()

  }
}
