package com.entrobus.statefulOperations

import java.sql.Timestamp

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.{Iterator, Seq, mutable}

/**
  * 环比
  * 计算每个零售户id，每个月的订单环比
  * 数据：
  *     id：零售户id
  *     month：月
  *     order：订单量
  */
object linkRelativeRatio {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("topN").master("local[3]").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    // id,month,order
    val df=spark.readStream.format("delta").load("e://test//delta//test")

    //将同一个月的日期，归并到同一个日期(1号)
    val df1=df.withColumn("date",date_trunc("month",$"date").cast("string"))
    val result=df1.groupByKey(row=>row.getAs[String]("city"))
      .flatMapGroupsWithState(OutputMode.Update(),GroupStateTimeout.NoTimeout)(linkRelativeRatio)

    result.writeStream.format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  def linkRelativeRatio(city:String,value:Iterator[Row],state:GroupState[State]):Iterator[Update]={
      if(state.hasTimedOut){//超时
          state.remove()
          Iterator()
      }else{//未超时
        //-----1.获取旧的状态
        val oldState=if(state.exists){
          state.get
        } else{
          State(city,mutable.Map[String,Long]())
        }

        //-----2.更新状态
        //获取旧的每个city的统计值
        val dateMaps=oldState.dateMaps

        //将新的数据根据日期预聚合
        val newValue=value.toSeq
        //根据city进行聚合
        val stringToRows: Map[String, Seq[Row]] = newValue.groupBy(_.getAs[String]("date"))
        //统计每个city的统计值
        val stringToLong: Map[String, Long] = stringToRows.map(v=>(v._1,v._2.map(_.getAs[Long]("order")).sum))


        //更新city的统计值
        stringToLong.foreach(v=>{
          val date=v._1
          val newOrder=v._2
          if(dateMaps.contains(date)){
            //包含就累加
            dateMaps(date)+=newOrder
          }else{
            //不包含就新增
            dateMaps.getOrElseUpdate(date,newOrder)
          }
        })
        //更新状态
        val newState=State(city,dateMaps)
        state.update(newState)


        //-----3.构建返回值，这里计算环比（本期-上期）/上期
        //这里使用joda这个日期处理类
        val dtf=DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        //将日期字符串转成日期类型
        val d=dateMaps.map(v=>(DateTime.parse(v._1,dtf),v._2))

        //遍历，根据本期日期减去一个月得到上期日期，使用上期日期去dateMaps里面获取它的统计值，如果没有返回0
        val r=d.map(v=>{
          val date=v._1
          val value=v._2

          //获取上期日期
          val lastMonth=date.minusMonths(1)
          //获取上期的统计值，没有给个默认值0
          val lastValue=d.getOrElse[Long](lastMonth,0)

         //如果分母为0(上期为0),返回空
          val a:Double=try{
                   ((value-lastValue)/lastValue).toDouble
                }catch{
                  case ex:Exception=>{Double.NaN}
                }
          Update(city,date.toString("yyyy-MM-dd"),a)
        }).toIterator
        r
      }
  }
  //状态    存储每个month的sum(order)
  case class State(city:String,dateMaps:mutable.Map[String,Long])
  //返回的值，city,month,link_relative_ratio
  case class Update(city:String,date:String,link_relative_ratio:Double)

}
