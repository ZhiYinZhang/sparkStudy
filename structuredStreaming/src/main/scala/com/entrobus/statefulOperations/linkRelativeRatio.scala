package com.entrobus.statefulOperations

import com.entrobus.statefulOperations.topN.topN
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

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
    val df=spark.readStream.format("delta").load("e://test//delta//test1")


    val result=df.groupByKey(row=>row.getAs[String]("province"))
      .flatMapGroupsWithState(OutputMode.Update(),GroupStateTimeout.NoTimeout)(topN)

    result.writeStream.format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }
  def topN(province:String,value:Iterator[Row],state:GroupState[State]):Iterator[Update]={
    val oldState=if(state.exists){
      state.get
    }else{
      State(province,mutable.Map[String,Long]())
    }
    //取出每个city的历史统计值
    val cityMaps=oldState.cityMaps

    //按照city分组   Seq[Row]记录了相同city的Row
    val stringToRows: Predef.Map[String, Seq[Row]] = value.toSeq.groupBy(row=>row.getAs[String]("city"))
    //统计Seq[Row]里面Row的value字段,得到 city->total
    val stringToLong: Predef.Map[String, Long] = stringToRows.map(v=>(v._1,v._2.map(_.getAs[Long]("value")).sum))

    stringToLong.foreach(v=>{
      val city=v._1
      val total=v._2
      //更新cityMaps
      if(cityMaps.contains(city)){//如果存在，累加
        cityMaps(city)+=total
      }else{//不存在，添加进去
        //如果key存在，返回value；如果不存在，将key添加进去，值为第二个参数，并返回该值
        cityMaps.getOrElseUpdate(city,total)
      }
    })

    //更新state
    val newState=State(province,cityMaps)
    state.update(newState)

    //将cityMaps排序，然后取topN
    val tuples: Seq[(String, Long)] = cityMaps.toSeq.sortWith(_._2<_._2).take(3)
    val updates: Seq[Update] = tuples.map(v => Update(province, v._1, v._2))
    updates.toIterator

  }
  //状态    存储每个month的sum(order)
  case class State(province:String,cityMaps:mutable.Map[String,Long])
  //返回的值，id,month,link_relative_ratio
  case class Update(province:String,city:String,total:Long)

}
