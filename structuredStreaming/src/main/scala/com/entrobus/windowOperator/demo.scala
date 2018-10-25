package com.entrobus

import java.sql.{Connection, PreparedStatement, Timestamp}
import java.util.{Date, Random, UUID}


import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}

import scala.collection.mutable
import scala.collection.mutable.Set
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

object demo {
  def main(args:Array[String]):Unit= {
        val row = Row(1,2,3,4,5,6)
    println(row.get(1))
  }
  def getUUID():String={
    UUID.randomUUID().toString()
  }
  def getUserId(userList:List[String]):String={
    val ran=new Random()
    val index=ran.nextInt(userList.length)
    val str=userList.apply(index)
    str
  }
  def getMap(num:Int):Map[String,String]={
    var map:Map[String,String]=Map()
    for(i <- 1 to  num){
        map+=(getUUID()->getUUID())
    }
    println("我执行了")
    map
  }

  def randomData(num:Int): String ={
    val ad_type=Array("banner","modal","sponsored-search","mail","mobile")
    val event_type=Array("view","click","purchase")
    val random = new Random()
    val i: Int = random.nextInt(num)
    if(num==event_type.length){
      return event_type(i)
    }else if(num==ad_type.length){
      return ad_type(i)
    }else
      return null
  }
}
