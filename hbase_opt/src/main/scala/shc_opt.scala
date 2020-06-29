import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object shc_opt {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("shc hbase")
//      .config("spark.files","e://test//hbase_dpd//hbase-site.xml")
      .master("local[2]").getOrCreate()

    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    sc.setLogLevel("WARN")

    val catalog=
      """{
        |"table":{"namespace":"default","name":"test"},
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
}
