import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.CreateHiveTableContext

object hiveOpt {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("hive")
      .master("local[2]")
     // .config("spark.files","/opt/cloudera/parcels/CDH-5.13.3-1.cdh5.13.3.p0.2/lib/hive/conf/hive-site.xml")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import spark.sql
    sql("show databases").show()
  }
}
