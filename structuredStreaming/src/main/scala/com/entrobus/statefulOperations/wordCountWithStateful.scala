package com.entrobus.statefulOperations


import java.sql.Timestamp

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, SparkSession}

object wordCountWithStateful {
  def main(args:Array[String]):Unit={
    val spark: SparkSession = SparkSession.builder()
      .appName("wordCount")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "10.18.0.19")
      .option("port", "9999")
      .option("includeTimestamp",true)
      .load()
    val splitDS: Dataset[(String, Timestamp)] = lines.as[(String,Timestamp)].flatMap(x=>{x._1.split(" ").map((_,x._2))})
    val keyValue: KeyValueGroupedDataset[String, (String, Timestamp)] = splitDS.groupByKey(_._1)
//    val value: KeyValueGroupedDataset[String, Long] = splitDS.groupByKey(_._1).mapValues(_._2.getTime())

    val result: Dataset[UpdateState] = keyValue.mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())((word: String, lines: Iterator[(String, Timestamp)], state: GroupState[UpdateState]) => {
     val processTime = new Timestamp(state.getCurrentProcessingTimeMs())
//      println(processTime)
      if (state.hasTimedOut) {
        val oldState: UpdateState = state.get
        val finalState: UpdateState =UpdateState(oldState.word,oldState.num,oldState.startTimestamp,oldState.endTimestamp,oldState.processTime,true)
        state.remove()
        finalState
      } else {
        val timestamps: Seq[Long] = lines.map(_._2.getTime).toSeq
        val updateValue:UpdateState = if (state.exists) {
          val oldState: UpdateState = state.get
          val newNum: Long = oldState.num + timestamps.size
          UpdateState(word,newNum,oldState.startTimestamp,new Timestamp(timestamps.max),processTime,false)
        } else {
          UpdateState(word,timestamps.size,new Timestamp(timestamps.min),new Timestamp(timestamps.max),processTime,false)
        }
        state.setTimeoutDuration("3 seconds")
        state.update(updateValue)
        updateValue
      }
    })


    result.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate",false)
      .start()
      .awaitTermination()

  }
  case class UpdateState(word:String,num:Long,startTimestamp:Timestamp,endTimestamp:Timestamp,processTime:Timestamp,timeout:Boolean)
}
