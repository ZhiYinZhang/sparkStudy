

import java.io.{BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream}
import java.lang
import java.time.{Clock, Instant, ZoneId}
import java.util.{Date, Properties}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerStageSubmitted, StageInfo}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.LongAccumulator
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.mutable
import scala.util.Random


//case class SimpleData(name:String,value:Double)


object demo1 {
  def main(args: Array[String]): Unit = {
      val spark=SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()
      import spark.implicits._
      val sc=spark.sparkContext
      sc.setLogLevel("WARN")

//    val path="e://test//delta//test1"
//    val df=spark.readStream.format("delta").load(path)
//
//    val window=Window.partitionBy("id").orderBy("date")
//
//    val df1=df.withColumn("value1",lead("value",1).over(window))
//
//    df1.writeStream.format("console").outputMode("append")
//      .start()
//      .awaitTermination()



  }




  def merge_sort(l:List[Double]):List[Double]={

    val length=l.length
    if(length<=1){
      return l
    }
    val middle=(length/2).toInt

    val left=merge_sort(l.slice(0,middle))
    val right=merge_sort(l.slice(middle,length))

    return merge(left,right)
  }
  def merge(left:List[Double],right:List[Double]):List[Double]={
    var result=List[Double]()
    var i=0
    var j=0
    while(i<left.length & j<right.length){
      if(left(i)<right(j)){
        result=result.:+(left(i))
        i+=1
      }else{
        result=result.:+(right(j))
        j+=1
      }
    }
    //两个长度不同的list,在跳出while之后,某一个list里面还有值
    result=result.++(left.slice(i,left.length))
    result=result.++(right.slice(j,left.length))

    return result
  }
}
