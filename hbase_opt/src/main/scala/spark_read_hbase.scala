import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.{Column, SparkSession}

object spark_read_hbase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("hive")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")



  }
}
