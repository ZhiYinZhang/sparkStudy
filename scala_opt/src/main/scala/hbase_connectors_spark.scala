

object hbase_connectors_spark {
  def main(args: Array[String]): Unit = {
      val l=List("a","b")
    var m=Map("a"->"int")


    val str = get_catalog("test","0","rowKey",m)
    print(str)
  }
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
      val c=keys(i)
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
