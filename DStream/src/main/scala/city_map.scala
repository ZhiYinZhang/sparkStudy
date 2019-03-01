import java.io.FileInputStream
import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties, UUID}

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.hadoop.yarn.util.Records
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从flume中读取数据
  * 写入到mysql
  */
object city_map {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("readFlume").setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(5))


    val host= getParam("host")
    val port= getParam("port").toInt
    val pollStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc,host,port)
//    val pushStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(ssc,host,port)

     val value: DStream[String] = pollStream.map(x=>new String(x.event.getBody.array()))

//      value.print()
//    value.saveAsTextFiles("E:\\test\\checkpoint\\flume")



    //
    value.foreachRDD(rdd=>{

      rdd.foreachPartition(iterator=>{
        val conn: Connection = getDBConnect()
        //        val sql="insert into flume(t) values(?)"
        val sql="insert into aggregation_data(id,count,longitude,latitude,data_time,add_time,pcode,pname,citycode,cityname) values(?,?,?,?,?,?,?,?,?,?)"

        var ps: PreparedStatement=conn.prepareStatement(sql)
        iterator.foreach(record=>{
          generate_data(ps,record)
          ps.executeUpdate()
        })
        conn.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def getDBConnect():Connection={
    val driver=getParam("driver")
    val jdbcUrl=getParam("jdbcUrl")
    val user=getParam("user")
    val password=getParam("password")

    val dataSource = new ComboPooledDataSource()
    dataSource.setDriverClass(driver)
    dataSource.setJdbcUrl(jdbcUrl)
    dataSource.setUser(user)
    dataSource.setPassword(password)
    dataSource.setInitialPoolSize(10)
    dataSource.setMaxPoolSize(10)
    dataSource.setMinPoolSize(1)
    dataSource.setAcquireIncrement(1)
    val connection: Connection = dataSource.getConnection
    connection
  }

  def generate_data(ps:PreparedStatement,record:String):PreparedStatement={

      val records: Array[String] = record.split(",")

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

    ps.setString(1,id)
    ps.setDouble(2,count)
    ps.setDouble(3,wgs_lng)
    ps.setDouble(4,wgs_lat)
    ps.setString(5,data_time)
    ps.setString(6,add_time)
    ps.setString(7,pcode)

    ps.setString(8,pname)
    ps.setString(9,citycode)
    ps.setString(10,cityname)

    ps
    }
   def getParam(key:String):String={
     val prop = new Properties()
     val path = Thread.currentThread().getContextClassLoader().getResource("city_map.properties").getPath
     prop.load(new FileInputStream(path))
     val value: String = prop.getProperty(key)
     value
   }
}
