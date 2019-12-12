import java.io

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.{DeleteRowsEventData, Event, EventData, EventHeader, EventHeaderV4, EventType, RotateEventData, UpdateRowsEventData, WriteRowsEventData}

object mysql_binlog {
  def main(args: Array[String]): Unit = {

//    val event=Map("WRITE_ROWS"->WriteRowsEventData,"DELETE_ROWS"->DeleteRowsEventData,"UPDATE_ROWS"->UpdateRowsEventData)


    val client = new BinaryLogClient("localhost",3306,"root","123456")
    client.setBinlogFilename("binlog.000001")
    client.setBinlogPosition(2090)

    client.registerEventListener(new BinaryLogClient.EventListener {
      override def onEvent(event: Event): Unit = {
        val header:EventHeader=event.getHeader()
        val eventType=header.getEventType().toString()

//        println(event.toString)
        if(eventType=="WRITE_ROWS"){
          val data:WriteRowsEventData=event.getData()

          val serializables: Array[io.Serializable] = data.getRows().get(0)

          println(serializables.mkString(",")+",false")
        }else if(eventType=="DELETE_ROWS"){
          val data:DeleteRowsEventData=event.getData()
          val serializables: Array[io.Serializable] = data.getRows().get(0)

          println(serializables.mkString(",")+",true")
        }else if(eventType=="UPDATE_ROWS"){
          val data:UpdateRowsEventData=event.getData()
//          val key: Array[io.Serializable] = data.getRows().get(0).getKey()
          val value: Array[io.Serializable] = data.getRows().get(0).getValue()


          println(value.mkString(",")+",false")
        }
//        println("\n\n\n")


//        println(header.getEventType().values().mkString(","))
      }
    })
    client.connect()
  }
}
