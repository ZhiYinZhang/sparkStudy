import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
/**
  * spark使用hortonworks的开源框架shc读写HBase,形成DataFrame
  */
object shc_opt_hbase {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("read_hbase")
      .master("local[3]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("WARN")

//    shc_read_hbase(spark)
//    shc_write_hbase(spark)
  }


  def shc_read_hbase(spark:SparkSession)={
    //定义hbase表的schema
    /**
      * """{
      * "table":{"namespace":"default","name":"test1"},
      * "rowkey":"key", 固定的
      * "columns":{ 键为DataFrame的列名,可以自定义；row key也必须定义为一列,它的cf是特定的rowkey,col也是固定的；值为一个map,指定要读取的列
      * "row_key":{"cf":"rowkey","col":"key","type":"string"},
      * "age":{"cf":"0","col":"age","type":"string"},
      * "name":{"cf":"0","col":"name","type":"string"}
      * }
      * }"""
      */
    val catalog=
      """{
        |"table":{"namespace":"default","name":"test1"},
        |"rowkey":"key",
        |"columns":{
          |"row_key":{"cf":"rowkey","col":"key","type":"string"},
          |"age":{"cf":"0","col":"age","type":"string"},
          |"name":{"cf":"0","col":"name","type":"string"}
        |}
      }""".stripMargin

    val df=spark.read.options(Map(HBaseTableCatalog.tableCatalog->catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    df.show()
  }
//
//  def shc_write_hbase(spark:SparkSession)={
//    //定义hbase表的schema
//    /**
//      * """{
//      * "table":{"namespace":"default","name":"test1"},
//      * "rowkey":"key", 固定的
//      * "columns":{ 键为DataFrame的列名,可以自定义；row key也必须定义为一列,它的cf是特定的rowkey,col也是固定的；值为一个map,指定要读取的列
//      * "row_key":{"cf":"rowkey","col":"key","type":"string"},
//      * "age":{"cf":"0","col":"age","type":"string"},
//      * "name":{"cf":"0","col":"name","type":"string"}
//      * }
//      * }"""
//      */
//    val catalog=
//      """{
//        |"table":{"namespace":"default","name":"test1"},
//        |"rowkey":"key",
//        |"columns":{
//          |"row_key":{"cf":"rowkey","col":"key","type":"string"},
//          |"age":{"cf":"0","col":"age","type":"string"},
//          |"name":{"cf":"0","col":"name","type":"string"}
//        |}
//      }""".stripMargin
//
//    val data=Seq(
//      ("0","11","jack"),
//      ("1","16","tom")
//    )
//
//    spark.createDataFrame(data).toDF("row_key","age","name").write
//       .options(Map(HBaseTableCatalog.tableCatalog->catalog,HBaseTableCatalog.newTable->"5"))//指定region的数量 必须>3
//       .format("org.apache.spark.sql.execution.datasources.hbase")
//       .save()
//  }

def get_catalog(table:String,cf:String,row_key:String,cols:Any)={
  /**
    * """{
    * |"table":{"namespace":"default", "name":"table1"},
    * |"rowkey":"key",
    * |"columns":{
    * |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
    * |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
    * |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
    * |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
    * |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
    * |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
    * |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
    * |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
    * |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
    * |}
    * |}""".stripMargin
    *
    */
  //cols为list时，默认所有列的类型为string
  val columns:Map[String,String]=cols match{
    case l:List[String]=>{
      var m=Map[String,String]()
      val cols1=cols.asInstanceOf[List[String]]
      for(c<-cols1){
        m.+=((c,"string"))
      }
      m
    }
    case m:Map[String,String]=>cols.asInstanceOf[Map[String,String]]
  }


  val keys: List[String] = columns.keys.toList

  var r=""
  for(i <- 0 until keys.size){
    //字段名称
    val c=keys(i)
    //字段类型
    val t=columns(c)

    var s=""
    if(i==keys.size-1){
      s=f"""|"${c}":{"cf":"$cf","col":"$c","type":"$t"}"""
    }else{
      s=f"""|"${c}":{"cf":"$cf","col":"$c","type":"$t"},\n"""
    }
    r+=s
  }

  val catalog=
    s"""{
       |"table":{"namespace":"default","name":"$table"},
       |"rowkey":"key",
       |"columns":{
       |"$row_key":{"cf":"rowkey","col":"key","type":"string"},
        $r
       |}
       |}""".stripMargin

  catalog
}

}
