import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Dstream_demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
   //读取hdfs目录，可以是put上去
  // val ds: DStream[String] = ssc.textFileStream("hdfs://10.18.0.28:8020/user/zhangzy/lgb/")
    //读取本地目录，文件必须是流式的写入的，不能复制粘贴进来
   val ds: DStream[String] = ssc.textFileStream("E:\\test\\csv\\stream")

  // val ds1: DStream[String] = ds.flatMap(_.split(","))



    ds.print(10)
    ssc.start()
    ssc.awaitTermination()
  }
}
