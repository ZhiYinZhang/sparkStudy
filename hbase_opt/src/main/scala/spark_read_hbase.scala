import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos
import org.apache.spark.sql.SparkSession

object spark_read_hbase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("hbase")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")



      System.setProperty("HADOOP_USER_NAME","hadoopUser")
      val conf =HBaseConfiguration.create()
      conf.set("hadoop.user.name","zhangzy")
      conf.set("hbase.zookeeper.property.clientPort","2181")
      conf.set("hbase.zookeeper.quorum","entrobus12,entrobus32,entrobus34")
      conf.set(TableInputFormat.INPUT_TABLE,"testtable")
      conf.set(TableInputFormat.SCAN_ROW_START,"row-1")
      conf.set(TableInputFormat.SCAN_ROW_STOP,"row-3")

      val hbaseRDD=sc.newAPIHadoopRDD(conf,classOf[TableInputFormat]
              ,classOf[ImmutableBytesWritable]
              ,classOf[org.apache.hadoop.hbase.client.Result])

    val l: Long = hbaseRDD.count()
     println(l)




  }
}
