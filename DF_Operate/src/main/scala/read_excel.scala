import org.apache.spark.sql.SparkSession
import com.crealytics.spark.excel._
object read_excel {
  def main(args:Array[String]):Unit={
      val spark=SparkSession.builder()
      .appName("read excel")
      .master("local[3]")
      .getOrCreate()

      import spark.implicits._
      val sc=spark.sparkContext
      sc.setLogLevel("WARN")
       spark.read.excel()

      val df= spark.read.format("com.crealytics.spark.excel")
      .option("dataAddress","'各主体数据汇总更新'!A2:C13") // 指定sheet 'my sheet'!   A2:C13表示范围
      .option("useHeader","true")
      .option("treatEmptyValuesAsNulls","false")
      .option("inferSchema","false")
//      .option("addColorColumns","true")
      .option("timestampFormat","MM-dd-yyyy HH:mm:ss")
      .option("maxRowsInMemory",20)
      .option("excerptSize",10)
      .option("workbookPassword",null)
      .load("e://test//千摩销售目标及回款2019(1).xlsx")

    df.printSchema()
    df.show()
  }
}
