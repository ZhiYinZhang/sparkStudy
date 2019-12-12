package com.entrobus.statefulOperations

import java.sql.Timestamp
import java.util.Date

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, Row, SparkSession}

object wordCountExample {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("wordCount")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

//    val lines: DataFrame = spark.readStream
//      .format("socket")
//      .option("host", "10.18.0.34")
//      .option("port", "9999")
//      .option("includeTimestamp",true)
//      .load()
//val splitDF=lines.flatMap(x=>x.getString(0).split(" ").map(y=>(y,x.getTimestamp(1)))).toDF("value","timestamp")
//    splitDF.printSchema()

    val lines=spark.readStream.format("delta").load("e://test//delta/test")



    val value: KeyValueGroupedDataset[String, Row] = lines.withWatermark("timestamp","3 seconds").groupByKey(_.getString(0))

    val result=value.mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(wordCount)


    result.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate",false)
      .start()
      .awaitTermination()
  }

  case class ValueState(word:String,num:Long,
                         startTimestamp:Timestamp,endTimestamp:Timestamp,processTime:Timestamp,timeout:Boolean)

  def wordCount(word:String,lines:Iterator[Row],state:GroupState[ValueState]):ValueState={
    /**
      * 判断是否超时
      *   超时，返回上一次的状态作为最终状态，并重置状态
      *   未超时，更新状态
      *       判断状态是否存在(即是否是第一次/或者超时后的第一次)
      *           状态存在，那么使用当前值去更新状态
      *           状态不存在，那么将使用当前值初始化状态
      */
      // Iterator使用size，将所有值迭代出来统计，再使用的话，就为空了
    val values=lines.toSeq

    println(f"watermark:${new Timestamp(state.getCurrentWatermarkMs())},processtime:${new Timestamp(state.getCurrentProcessingTimeMs())}")

    val processTime = new Timestamp(state.getCurrentProcessingTimeMs())

    if (state.hasTimedOut) { //超时
      //返回上一次的状态，并重置
      val lastState = state.get
      val finalState = ValueState(word, lastState.num, lastState.startTimestamp, lastState.endTimestamp, lastState.processTime, true)
      state.remove()
      finalState
    } else {
      val num = values.size
      //该group所有eventTime
      val timestamps: Seq[Long] = values.map(_.getTimestamp(1).getTime)

      val minTimestamp=new Timestamp(timestamps.min)
      val maxTimestamp=new Timestamp(timestamps.max)

      val newState = if (state.exists) {
        val lastState = state.get
        //考虑有延期的数据
//        val startTimestamp=new Timestamp(List(minTimestamp.getTime,lastState.startTimestamp.getTime).min)
        //考虑有延期的数据
        val endTimestamp=new Timestamp(List(maxTimestamp.getTime,lastState.endTimestamp.getTime).max)

        ValueState(word, lastState.num + num, lastState.startTimestamp, endTimestamp, processTime, false)
      } else {
        ValueState(word, num, minTimestamp, maxTimestamp, processTime, false)
      }

      //退出条件： timestamp.max < watermark-10s
//      state.setTimeoutTimestamp(timestamps.max,"10 seconds")
      //退出条件：timestmap.min < watermark
//      state.setTimeoutTimestamp(timestamps.min)
      //该key所有事件时间的最大值
//      state.setTimeoutTimestamp(newState.endTimestamp.getTime)
      //该key所有事件时间的最小值（不是第一次事件时间）
      state.setTimeoutTimestamp(newState.startTimestamp.getTime,"10 seconds")
//      state.setTimeoutDuration("1 minute")
      state.update(newState)
      newState
    }
  }
}
