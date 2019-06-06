import java.{lang, util}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellScanner, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object spark_read_hbase_non_kerberos {
  val spark=SparkSession.builder()
    .appName("read_hbase")
    .master("local[3]")
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext
  import spark.implicits._
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {




    val conf: Configuration = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", "10.72.59.89") // zookeeper地址
//    conf.set("hbase.zookeeper.property.clientPort", "2181") // zookeeper端口
//    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set(TableInputFormat.INPUT_TABLE,"test1")
    //    conf.set(TableInputFormat.SCAN_ROW_START,"row-1")
    //    conf.set(TableInputFormat.SCAN_ROW_STOP,"row-9")
    //获取  列族 colfam1  列 col-1的值
//    conf.set(TableInputFormat.SCAN_COLUMNS,"0:age")


    val hbaseRDD=sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

//    hbaseRDD.foreach(tuple=>{
//      val row_key: ImmutableBytesWritable = tuple._1
//
//      val value: Result = tuple._2
//
//      val cells: Array[Cell] = value.rawCells()
//      for(cell <- cells) {
//        println(Bytes.toString(cell.getRow),Bytes.toString(cell.getFamily),Bytes.toString(cell.getQualifier),Bytes.toString(cell.getValue))
////        val bytes: Array[Byte] = cell.getTagsArray()
////        println(Bytes.toString(bytes))
////        println(Bytes.toString(cell.getRowArray),Bytes.toString(cell.getFamilyArray),Bytes.toString(cell.getQualifierArray))
//
//      }
//
//    })



//     hbaseRDD_to_df(hbaseRDD,"0")

  }

  def hbaseRDD_to_df(hbaseRDD:RDD[(ImmutableBytesWritable, Result)],columnFamily:String)={
    val columns=hbaseRDD.map(tuple=>{
      val result: Result = tuple._2
      val cells: Array[Cell] = result.rawCells()

      var columns=List[String]()
      for(cell<-cells){
        val row = Bytes.toString(cell.getValueArray).trim()
        val strings: Array[String] = row.split("\\W+")

        val column=strings(1).split(columnFamily)(1)
        columns=columns.:+(column)
      }
      columns
    }).take(1)

    columns.foreach(println)


    val rdd1=hbaseRDD.map(tuple=>{
      val row_key= Bytes.toString(tuple._1.get())
      val result: Result = tuple._2

      val cells: Array[Cell] = result.rawCells()

      var value=List(row_key,columnFamily)
      for(cell<-cells){
          val row = Bytes.toString(cell.getValueArray).trim()
          val strings: Array[String] = row.split("\\W+")
          value=value.:+(strings(strings.length-1))
      }

      (value(0),value(1),value(2),value(3))
    })




    spark.createDataFrame(rdd1).show()
  }
}

