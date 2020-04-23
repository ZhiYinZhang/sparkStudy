package statefulOperations

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

object statefulExample {
  def main(args:Array[String]):Unit={
    val spark: SparkSession = SparkSession.builder
      .appName("statefulExample")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val lines=spark.readStream
      .format("socket")
      .option("host","10.18.0.19")
      .option("port","9999")
      .option("includeTimestamp",true)
      .load()
    //将行拆分为单词，每个单词为事件的sessionId
    val events: Dataset[Event] = lines.as[(String, Timestamp)]
      .flatMap { case (line, timestamp) => line.split(" ").map(word => Event(sessionId = word, timestamp)) }

    //    val value: KeyValueGroupedDataset[String, Event] = events.groupByKey(_.sessionId)
    val sessionUpdate: Dataset[SessionUpdate] = events.groupByKey(event => event.sessionId)//KeyValueGroupedDataset[String,Event]
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.NoTimeout())((sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) => {
      println(sessionId+":"+state.getCurrentProcessingTimeMs())
      //如果超时，则删除会话并发送最终的更新状态
      if (state.hasTimedOut) {
        val finalUpdate: SessionUpdate = SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
        state.remove()
        finalUpdate
      } else {
        //更新会话中的开始和结束时间戳
        val timestamps: Seq[Long] = events.map(_.timestamp.getTime).toSeq
        val updateSessionInfo: SessionInfo = if (state.exists) {//存在并且不是第一次
          //设置新状态
          val oldSessionInfo: SessionInfo = state.get
          SessionInfo(oldSessionInfo.numEvents + timestamps.size,
            oldSessionInfo.startTimestampMs,
            math.max(oldSessionInfo.endTimestampMs, timestamps.max))
        } else {//设置初始状态
          SessionInfo(timestamps.size, timestamps.min, timestamps.max)
        }
        //更新状态
        state.update(updateSessionInfo)
        //设置超时，如果10秒内没有收到数据，会话将过期
        state.setTimeoutDuration("3 seconds")
//        println(sessionId+":"+state.getCurrentProcessingTimeMs())
//        state.getCurrentWatermarkMs()
        SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
      }
    })
    //开始运行Query，将sessionUpdate打印到控制台
    sessionUpdate.writeStream
      .outputMode("update")
      .format("console")
      .start()
      .awaitTermination()
  }
}
//表示输入事件的用户定义的数据类型
case class Event(sessionId:String,timestamp:Timestamp)
//在mapGroupsWithState中存储会话信息的用户自定义的数据类型
case class SessionInfo(
                        numEvents:Int,  //会话中收到的事件总数
                        startTimestampMs:Long, //会话中收到的第一个事件的时间戳
                        endTimestampMs:Long){ //会话到期前收到的最后一个事件的时间戳
  //会话的持续时间，在第一次和最后一次活动之间
  def durationMs:Long=endTimestampMs-startTimestampMs
}
//用户定义的数据类型，表示mapGroupsWithState返回的更新信息
case class SessionUpdate(
                          sessionId:String,//sessionId
                          durationMs:Long,//会话处于活动状态的持续时间，即从第一个事件到其到期
                          numEvents:Int,//会话处于活动状态时收到的事件数
                          expired:Boolean) //会话是活动还是已过期
