import org.apache.spark.sql.{DataFrame, SparkSession}
object spark_phoenix {
  def main(args: Array[String]): Unit = {
    println(this.getClass.getResource("zhangzy.keytab").getFile)
  }
}
