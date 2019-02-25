package org.apache.spark.streaming.recriver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * 自定义socket数据源
  */
class customRecriver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging{
  override def onStart(): Unit = {
    //开启一个线程 去接收数据
    new Thread("socket receiver"){
      override def run(): Unit ={
        receive()
      }
    }.start()
  }

  override def onStop(): Unit ={

  }

  /***
    * 创建socket连接并接收数据 直到停止
    */
  private def receive(): Unit ={
    var socket:Socket = null
    var userInput:String=null
    try{
      //连接socket
      socket = new Socket(host,port)

      new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.UTF_8))
    }
  }
}
