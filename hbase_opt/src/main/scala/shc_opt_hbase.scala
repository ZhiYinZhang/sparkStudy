import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
/**
  * spark使用hortonworks的开源框架shc读写HBase,形成DataFrame
  */
object shc_opt_hbase {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("read_hbase")
      .master("local[3]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("WARN")

//    shc_read_hbase(spark)
    shc_write_hbase(spark)
  }


  def shc_read_hbase(spark:SparkSession)={
    //定义hbase表的schema
    /**
      * """{
      * "table":{"namespace":"default","name":"test1"},
      * "rowkey":"key", 固定的
      * "columns":{ 键为DataFrame的列名,可以自定义；row key也必须定义为一列,它的cf是特定的rowkey,col也是固定的；值为一个map,指定要读取的列
      * "row_key":{"cf":"rowkey","col":"key","type":"string"},
      * "age":{"cf":"0","col":"age","type":"string"},
      * "name":{"cf":"0","col":"name","type":"string"}
      * }
      * }"""
      */
    val catalog=
      """{
        |"table":{"namespace":"default","name":"test1"},
        |"rowkey":"key",
        |"columns":{
          |"row_key":{"cf":"rowkey","col":"key","type":"string"},
          |"age":{"cf":"0","col":"age","type":"string"},
          |"name":{"cf":"0","col":"name","type":"string"}
        |}
      }""".stripMargin

    val df=spark.read.options(Map(HBaseTableCatalog.tableCatalog->catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    df.show()
  }

  def shc_write_hbase(spark:SparkSession)={
    //定义hbase表的schema
    /**
      * """{
      * "table":{"namespace":"default","name":"test1"},
      * "rowkey":"key", 固定的
      * "columns":{ 键为DataFrame的列名,可以自定义；row key也必须定义为一列,它的cf是特定的rowkey,col也是固定的；值为一个map,指定要读取的列
      * "row_key":{"cf":"rowkey","col":"key","type":"string"},
      * "age":{"cf":"0","col":"age","type":"string"},
      * "name":{"cf":"0","col":"name","type":"string"}
      * }
      * }"""
      */
    val catalog=
      """{
        |"table":{"namespace":"default","name":"test1"},
        |"rowkey":"key",
        |"columns":{
          |"row_key":{"cf":"rowkey","col":"key","type":"string"},
          |"age":{"cf":"0","col":"age","type":"string"},
          |"name":{"cf":"0","col":"name","type":"string"}
        |}
      }""".stripMargin

    val data=Seq(
      ("0","11","jack"),
      ("1","16","tom")
    )

    spark.createDataFrame(data).toDF("row_key","age","name").write
       .options(Map(HBaseTableCatalog.tableCatalog->catalog,HBaseTableCatalog.newTable->"5"))//指定region的数量 必须>3
       .format("org.apache.spark.sql.execution.datasources.hbase")
       .save()
  }
}
