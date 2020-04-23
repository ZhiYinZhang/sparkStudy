package foreachSink

import java.sql.{Connection, PreparedStatement, Timestamp}
import org.apache.spark.sql.functions._
import generateData._
import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}


object writeToMysql {
  def main(args:Array[String]):Unit= {

    val rows = 100
    //获取sparkSession对象
    val spark: SparkSession = SparkSession.builder
      .appName("continuousTrigger")
      .master("local[2]")
      .getOrCreate()
    //设置日志输出级别
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    var batchId: Long = 0
    //对查询添加一个监听，获取每个批次的处理信息
    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val progress: StreamingQueryProgress = event.progress
        batchId = progress.batchId
        val inputRowsPerSecond: Double = progress.inputRowsPerSecond
        val processRowsPerSecond: Double = progress.processedRowsPerSecond
        val numInputRows: Long = progress.numInputRows
        println("batchId=" + batchId, "  numInputRows=" + numInputRows + "  inputRowsPerSecond=" + inputRowsPerSecond +
          "  processRowsPerSecond=" + processRowsPerSecond)
        writeToFile(batchId+","+inputRowsPerSecond+","+processRowsPerSecond,"e://test//rate//uuid1.csv")
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })

    //读取数据源
    val rateDF: DataFrame = spark.readStream
      .format("rate")
      .option("rowsPerSecond", rows)
      .load()

    val map=getMap(1000)//  userId->ip
    var userIdList:List[String]=null
    var userId:String=null
    var ip:String=null
    //增加字段batchId  user_id page_id ad_id ad_type event_type ip
    val addDS = rateDF.as[(Timestamp, Long)].map(oldTuple=>{
      userIdList=map.keySet.toList
      userId=getRandomUserId(userIdList)
      ip=map.get(userId).get
      val tuple = (oldTuple._1, oldTuple._2, batchId + 1,
        userId, getUUID(), getUUID(), randomData(5),randomData(3),ip )
      tuple
    })
    val addDF: DataFrame = addDS.toDF("event_time", "num","batchId",
      "user_id","page_id","ad_id","ad_type","event_type","ip")


    val filterDS: Dataset[Row] = addDF.filter(!_.get(7).equals("view"))
     /* val resultDS: Dataset[Row] = filterDS
        .groupBy(window($"event_time", "4 seconds", "4 seconds"),$"event_type")
        .count()
        .orderBy($"window".desc)*/

    filterDS.writeStream
      .outputMode("complete")
      .foreach(new mysqlSink())
      //      .trigger(Trigger.ProcessingTime(2000))
      .start()
      .awaitTermination()
  }



  class mysqlSink extends ForeachWriter[Row](){
    var conn:Connection=null
    var ps:PreparedStatement=null
    var dataSource:ComboPooledDataSource=_
    val sql="insert into rate(event_time,num,batchId,user_id,page_id,ad_id,ad_type,event_type,ip) values(?,?,?,?,?,?,?,?,?)"
//    val sql="insert into aggregate(window,event_type,count) values(?,?,?)"
    override def open(partitionId: Long, version: Long): Boolean ={
      dataSource = new ComboPooledDataSource()
      dataSource.setDriverClass("com.mysql.jdbc.Driver")
      dataSource.setJdbcUrl("jdbc:mysql://localhost:3306/entrobus")
      dataSource.setUser("root")
      dataSource.setPassword("root")
      dataSource.setInitialPoolSize(40)
      dataSource.setMaxPoolSize(100)
      dataSource.setMinPoolSize(10)
      dataSource.setAcquireIncrement(5)
      conn= dataSource.getConnection
      ps= conn.prepareStatement(sql)
      return true
    }
    override def process(value: Row): Unit ={
      ps.setObject(1,value.get(0))
      ps.setObject(2,value.get(1))
      ps.setObject(3,value.get(2))
      ps.setObject(4,value.get(3))
      ps.setObject(5,value.get(4))
      ps.setObject(6,value.get(5))
      ps.setObject(7,value.get(6))
      ps.setObject(8,value.get(7))
      ps.setObject(9,value.get(8))
      val i: Int = ps.executeUpdate()
//      println(i+" "+String.valueOf(value.get(0))+" "+value.get(1)+" "+value.get(2))
      println(i+" "+value.get(0)+" "+value.get(1)+" "+value.get(2)+" "+value.get(3)+" "+value.get(4)+" "+value.get(5)
        +" "+value.get(6)+" "+value.get(7)+" "+value.get(8))
    }
    override def close(errorOrNull: Throwable): Unit = {
      println("-------------------------stop------------------------------")
      dataSource.close()
    }
  }
}
