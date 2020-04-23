package foreachSink
import java.sql.Timestamp
import generateData._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


object writeToRedis {
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
        writeToFile(batchId+","+numInputRows+"," +inputRowsPerSecond+","+processRowsPerSecond,"e://test//rate//redis.csv")
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })

    //读取数据源
    val rateDF: DataFrame = spark.readStream
      .format("rate")
      .option("rowsPerSecond", rows)
      .load()

    rateDF.repartition(1)
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
      val resultDS: Dataset[Row] = filterDS
          .withWatermark("event_time","4 seconds")
        .groupBy(window($"event_time", "4 seconds", "4 seconds"),$"event_type")
        .count()
        .orderBy($"window".desc)
    /*addDF.createOrReplaceTempView("test")
    val resultDF: DataFrame = spark.sql("select batchId,count(1) count from test group by batchId order by batchId")
   */

    resultDS.writeStream
//      .outputMode("complete")
//        .format("console")
      .foreach(new redisSink())
//      .trigger(Trigger.ProcessingTime(2000))
      .start()
      .awaitTermination()
  }
  class redisSink extends ForeachWriter[Row](){
    var jedis:Jedis=null
    override def open(partitionId: Long, version: Long): Boolean ={
//      println("--------------------------start-----------------------------")
      val config: JedisPoolConfig = new JedisPoolConfig()
      config.setMaxTotal(20)
      config.setMaxIdle(5)
      config.setMaxWaitMillis(1000)
      config.setMinIdle(2)
      config.setTestOnBorrow(false)
      val jedisPool = new JedisPool(config,"127.0.0.1",6379)
      jedis=jedisPool.getResource()
      return true
    }
    override def process(value: Row): Unit ={
      /*jedis.rpush("rate",value.get(0)+" "+value.get(1)+" "+value.get(2)+" "+value.get(3)+" "+value.get(3)+" "+value.get(4)+" "+value.get(5)
        +" "+value.get(6)+" "+value.get(7)+" "+value.get(8))
      println(value.get(0)+" "+value.get(1)+" "+value.get(2)+" "+value.get(3)+" "+value.get(3)+" "+value.get(4)+" "+value.get(5)
        +" "+value.get(6)+" "+value.get(7)+" "+value.get(8))*/
      jedis.rpush("rate",value.get(0)+" "+value.get(1)+" "+value.get(2))
      println(value.get(0)+" "+value.get(1)+" "+value.get(2))
    }
    override def close(errorOrNull: Throwable): Unit = {
//      println("-------------------------stop------------------------------")
      jedis.close()
    }
  }


}

