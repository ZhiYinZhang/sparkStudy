import java.text.SimpleDateFormat
import java.util
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}

import org.apache.spark.streaming.{Seconds, StreamingContext}


object read_flume {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("readFlume").setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(5))


    val host= "10.18.0.28"
    val port = 10001

    val pollStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc,host,port)
    //    val pushStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(ssc,host,port)

    val value: DStream[(util.Map[CharSequence, CharSequence], String)] = pollStream.map(x=>(x.event.getHeaders,new String(x.event.getBody.array())))
    val value1: DStream[(String, String)] = value.map(x => {
      val strings: Array[String] = x._1.get("file").toString.split("/")
      val fileName: String = strings(strings.length-1)
      val name = fileName.split("\\d")(0)
      (name, x._2)
    })

    value1.print()
    ssc.start()
    ssc.awaitTermination()


  }

}
