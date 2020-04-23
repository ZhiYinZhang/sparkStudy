package MicroBatchVSContinous

import java.io.{File, FileInputStream}
import java.util.{Properties, Random, UUID}

import org.apache.commons.io.FileUtils

import scala.collection.mutable.Map

object generateData {
  //传入.properties文件路径，返回Properties对象
  def readProperties(propertiesPath:String):Properties={
    val prop: Properties = new Properties()
    val inputStream = new FileInputStream(propertiesPath)
    prop.load(inputStream)
    inputStream.close()
    prop
  }
  def writeToFile(data:String,dest:String):Unit={
    val destination = new File(dest)
    FileUtils.writeStringToFile(destination,data+"\n",true)
  }
  def deleteDir(path:String):Unit={
    val file = new File(path)
    FileUtils.deleteDirectory(file)
  }
  def getUUID():String={
    UUID.randomUUID().toString()
  }
  //从传入的userId列表中随机返回一个
  def getRandomUserId(userList:List[String]):String={
    val ran=new Random()
    val index=ran.nextInt(userList.length)
    val str=userList.apply(index)
    str
  }
  //返回一个具有num个元素的userId->ip键值对
  def getMap(num:Int):Map[String,String]={
    var map:Map[String,String]=Map()
    for(i <- 1 to  num){
      map+=(getUUID()->getUUID())
    }
    map
  }
  //
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
