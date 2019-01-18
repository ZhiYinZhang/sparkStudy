import org.apache.spark.sql.{DataFrame, SparkSession}
object spark_phoenix {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("hive")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val jdbcPhoenixUrl = "jdbc:phoenix:entrobus12:2181"
    val tableName = "employee"

    val df: DataFrame = spark.sqlContext.load("org.apache.phoenix.spark",
      Map("table" -> tableName, "zkUrl" -> jdbcPhoenixUrl))
    df.show()
  }
}
