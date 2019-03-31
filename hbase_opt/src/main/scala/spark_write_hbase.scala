import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object spark_write_hbase {
  def main(args: Array[String]): Unit = {
    val krb5_conf = this.getClass.getResource("krb5.conf").getPath
    val user_keytab = this.getClass.getResource("zhangzy.keytab").getPath
    System.setProperty("java.security.krb5.conf", krb5_conf)

    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hadoop.security.authentication", "Kerberos")
    //    conf.set(TableOutputFormat.OUTPUT_TABLE,"test_ma")

    UserGroupInformation.setConfiguration(conf)
    val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("zhangzy@HADOOP.COM", user_keytab)
    UserGroupInformation.setLoginUser(ugi)

    val spark = SparkSession.builder().appName("read_hbase")
      .master("local[3]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("WARN")

    val df: DataFrame = spark.range(1000).withColumn("index", monotonically_increasing_id())
    //


    val job: Job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    val jobConf=job.getConfiguration
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"test_ma")

    df.rdd.map(x=>{
      val put = new Put(Bytes.toBytes(x.getLong(0).toString))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("number"),Bytes.toBytes(x.getLong(1).toString))
      (new ImmutableBytesWritable,put)
    }).saveAsNewAPIHadoopDataset(jobConf)


    sc.stop()

  }
}
