import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, SparkSession}
//spark version 1.6.0
object spark_read_hbase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("hive")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val zk="10.18.0.12:2181,10.18.0.32:2181,10.18.0.34:2181"
    val config = new HBaseConfiguration()
    config.set(HConstants.ZOOKEEPER_QUORUM,zk)
    val scan=new Scan()

   val tableName: TableName = TableName.valueOf("testtable")
    val hbaseContext: HBaseContext = new HBaseContext(sc,config)

     val rdd: RDD[(ImmutableBytesWritable, Result)] = hbaseContext.hbaseRDD(tableName,scan)
//
    rdd.toDF().show()
  }
}
