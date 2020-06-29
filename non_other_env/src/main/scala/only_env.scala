
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.joda.time.DateTime
/**
  * 测试只有这一个环境的，没有其他无关的依赖
  */
object only_env {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("shc hbase")
//      .config("spark.hadoop.validateOutputSpecs",false)
      .master("local[2]").getOrCreate()

    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("WARN")

    val hbaseConf=HBaseConfiguration.create()

    val hbaseContext=new HBaseContext(sc,hbaseConf)

    val catalog=
      """{
        |"table":{"namespace":"default","name":"test"},
        |"rowkey":"key",
        |"columns":{
          |"id":{"cf":"rowkey","col":"key","type":"string"},
          |"age":{"cf":"0","col":"age","type":"string"},
          |"name":{"cf":"0","col":"name","type":"string"}
        |}
      }""".stripMargin

    val df=spark.read
      .options(Map(HBaseTableCatalog.tableCatalog->catalog))
      .format("org.apache.hadoop.hbase.spark")
      .load()
    df.show()


//     spark.range(10).show()

//
//    val data=Seq(("1","1","a","1"),("2","2","b","2"),("3","3","c","3"),("4","4","d","4"))
//    val df=spark.createDataFrame(data).toDF("row_key","age","name","age1")
//      val df=spark.range(1).selectExpr("cast(id as string)").withColumn("age",lit("1")).withColumn("name",lit("shc"))
//
//      df.show()


  }
}
