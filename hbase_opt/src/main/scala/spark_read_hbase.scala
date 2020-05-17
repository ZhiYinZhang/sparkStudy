import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * spark读取Kerberos下的hbase
  */
object spark_read_hbase {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = get_conf_and_login()

    val spark=SparkSession.builder().appName("read_hbase")
      .master("local[3]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("WARN")


    /**
      *
    结果为Tuple2(Array[Byte],Result),即(rowKey,Result)
    (72 6f 77 2d 31,keyvalues={row-1/colfam1:col-1/1546997916572/Put/vlen=7/seqid=0, row-1/colfam1:col-10/1546997917118/Put/vlen=8/seqid=0, row-1/colfam1:col-2/1546997916653/Put/vlen=7/seqid=0, row-1/colfam1:col-3/1546997916711/Put/vlen=7/seqid=0, row-1/colfam1:col-4/1546997916770/Put/vlen=7/seqid=0, row-1/colfam1:col-5/1546997916828/Put/vlen=7/seqid=0, row-1/colfam1:col-6/1546997916885/Put/vlen=7/seqid=0, row-1/colfam1:col-7/1546997916943/Put/vlen=7/seqid=0, row-1/colfam1:col-8/1546997917002/Put/vlen=7/seqid=0, row-1/colfam1:col-9/1546997917059/Put/vlen=7/seqid=0, row-1/colfam2:col-1/1546997916572/Put/vlen=7/seqid=0, row-1/colfam2:col-10/1546997917118/Put/vlen=8/seqid=0, row-1/colfam2:col-2/1546997916653/Put/vlen=7/seqid=0, row-1/colfam2:col-3/1546997916711/Put/vlen=7/seqid=0, row-1/colfam2:col-4/1546997916770/Put/vlen=7/seqid=0, row-1/colfam2:col-5/1546997916828/Put/vlen=7/seqid=0, row-1/colfam2:col-6/1546997916885/Put/vlen=7/seqid=0, row-1/colfam2:col-7/1546997916943/Put/vlen=7/seqid=0, row-1/colfam2:col-8/1546997917002/Put/vlen=7/seqid=0, row-1/colfam2:col-9/1546997917059/Put/vlen=7/seqid=0})
      **/
    val hbaseRDD=sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    println(hbaseRDD.count())

    //获取rowkey
    val rowkeyRDD=hbaseRDD.map(tuple=>tuple._1)
                      .map(item=>Bytes.toString(item.get()))

    rowkeyRDD.foreach(println)
    //获取Result
    val resultRDD=hbaseRDD.map(_._2)
    val keyValueRDD= resultRDD.map(result=>{
      val bytes: Array[Byte] = result.value()
      val rt=Bytes.toString(bytes)
      println(rt)
      rt
    })


    resultRDD.foreach(result=>{
      val values: Array[KeyValue] = result.raw()
      for(value<-values){
        //只获取rowkey   还有value.getFamily   value.getValue
        //            println(new String(value.getKey))

        // "      row-9  colfam1col-1   h0B�z val-9.1"
        //       rowKey:row-9  列族:colfam1 列:col-1 值:val-9.1
        val row=new String(value.getValueArray)
        // \\w:匹配字母或数字或下划线或汉字 -:匹配“-”  \\.:匹配“.”  ^ :取反
        val strings: Array[String] =row.trim.split("[^\\w-\\.]+")

        //和getValueArray的值是一样的
        //            println(new String(value.getFamilyArray))
        //            println(new String(value.getRowArray))
        //            println(new String(value.getTagsArray))
        //            println(new String(value.getQualifierArray))

        println(strings(0),strings(1),strings(strings.size-1))
      }

    })

  }

def get_conf_and_login():Configuration={
  // 文件在resources目录下，但是需要在编译后添加到target/classes目录下
  val krb5_conf=this.getClass.getResource("krb5.conf").getPath
  val user_keytab=this.getClass.getResource("zhangzy.keytab").getPath
//  this.getClass.getResource("zhangzy.keytab")
  System.setProperty("java.security.krb5.conf", krb5_conf)

  val conf: Configuration = HBaseConfiguration.create()
  conf.set("hadoop.security.authentication", "Kerberos")
  //    conf.set("hbase.zookeeper.quorum", "10.18.0.12") // zookeeper地址
  //    conf.set("hbase.zookeeper.property.clientPort", "2181") // zookeeper端口
  //    conf.set("hbase.security.authentication", "Kerberos")
  //    conf.set("zookeeper.znode.parent", "/hbase")
      conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@HADOOP.COM")
      conf.set("hbase.master.kerberos.principal", "hbase/entrobus12@HADOOP.COM")
  conf.set(TableInputFormat.INPUT_TABLE,"test_ljs")
  //    conf.set(TableInputFormat.SCAN_ROW_START,"row-1")
  //    conf.set(TableInputFormat.SCAN_ROW_STOP,"row-9")
  //获取  列族 colfam1  列 col-1的值
//  conf.set(TableInputFormat.SCAN_COLUMNS,"colfam1:col-1")


  try{
    //设置登录的用户

    UserGroupInformation.setConfiguration(conf)
    val ugi=UserGroupInformation.loginUserFromKeytabAndReturnUGI("zhangzy@HADOOP.COM",user_keytab)
    UserGroupInformation.setLoginUser(ugi)
  }catch{
    case e:Exception=>{
      println(e.getMessage)
    }
  }
  conf
}
}
