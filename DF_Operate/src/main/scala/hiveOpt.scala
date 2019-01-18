import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.CreateHiveTableContext

object hiveOpt {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("hive")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import spark.sql
    sql("show tables").show()
//    sql("show databases").show()
    sql("select * from hivefromhbase").show()
  }
}
