import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}

object spark_read_hbase_non_kerberos {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
                          .appName("read_hbase")
                          .master("local[3]")
                          .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("WARN")



    val conf: Configuration = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", "10.72.32.26") // zookeeper地址
//    conf.set("hbase.zookeeper.property.clientPort", "2181") // zookeeper端口
//    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set(TableInputFormat.INPUT_TABLE,"test1")
    //    conf.set(TableInputFormat.SCAN_ROW_START,"row-1")
    //    conf.set(TableInputFormat.SCAN_ROW_STOP,"row-9")
    //获取  列族 colfam1  列 col-1的值
//    conf.set(TableInputFormat.SCAN_COLUMNS,"0:age,0:name")



    val hbaseRDD=sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    hbaseRDD.foreach(tuple=>{
      val row_key: ImmutableBytesWritable = tuple._1

      val value: Result = tuple._2


      val cells: Array[Cell] = value.rawCells()
      for(cell <- cells) {
        println(Bytes.toString(cell.getRow),Bytes.toString(cell.getFamily),Bytes.toString(cell.getQualifier),Bytes.toString(cell.getValue))
//        val bytes: Array[Byte] = cell.getTagsArray()
//        println(Bytes.toString(bytes))
//        println(Bytes.toString(cell.getRowArray),Bytes.toString(cell.getFamilyArray),Bytes.toString(cell.getQualifierArray))

      }

    })



  }
}
