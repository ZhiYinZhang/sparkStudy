import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties, UUID}

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import shapeless.record

object city_map2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("city_map2").setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(3))
    val host=new utils().getParam("host")
    val port =new utils().getParam("port").toInt
    //spark使用poll的方式去flume拉取数据
    val flumeStr: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc,host,port)


    val tuple: DStream[(String, Double, Double, Double, String, String, String, String, String, String,String,String)] = flumeStr.map(event => {
//      val paths: Array[String] = event.event.getHeaders.get("file").toString.split("/")
      //  文件命名 test20190227_102123.txt
//      val fileName: String = paths(paths.length-1).split("\\d")(0)

      val str = new String(event.event.getBody().array())
      val tuple: (String, Double, Double, Double, String, String, String, String, String, String,String,String) = format_data(str)
      tuple
    })

    tuple.foreachRDD(rdd=>{
      //创建sparkSession，导入隐式转换
      val spark: SparkSession = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df: DataFrame = rdd.toDF("id","count","longitude","latitude","data_time","add_time","pcode","pname","citycode","cityname","adcode","adname")
      df.show()
      writeJDBC(df)
    })

    ssc.start()
    ssc.awaitTermination()
  }
  def writeJDBC(df:DataFrame): Unit ={
    val utils = new utils()
    val jdbcUrl: String = utils.getParam("jdbcUrl")
    val table=utils.getParam("table")
    df.write.mode("Append").jdbc(jdbcUrl,table,utils.getProp("city_map.properties"))
  }
  def format_data(record:String): (String,Double,Double,Double,String,String,String,String,String,String,String,String)={
      val records: Array[String] = record.split(",")
      //csv文件中包含header
//      if (records(3)!="data_time"){
//
//      }
      val count = records(0).toDouble
      val wgs_lng=records(1).toDouble
      val wgs_lat=records(2).toDouble
      val data_time=records(3)

      val id = UUID.randomUUID().toString()
      val add_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())

      val pcode="430000"
      val pname="湖南省"
      val citycode="430200"
      val cityname="株洲市_test"
    val tuple: (String, Double, Double, Double, String, String, String, String, String, String,String,String) = (id,count,wgs_lng,wgs_lat,data_time,add_time,pcode,pname,citycode,cityname,null,null)
    tuple
  }
  val name_map=("zhuzhou"->"株洲市","changsha"->"长沙市","changde"->"常德市","hengyang"->"衡阳市","yueyang"->"岳阳市")
}
