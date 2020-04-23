

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
import org.apache.spark.sql.functions._
import scala.collection.mutable
import scala.util.Random


//case class SimpleData(name:String,value:Double)


object demo1 {
  def main(args: Array[String]): Unit = {
      val spark=SparkSession.builder()
      .appName("test")
      .master("local[*]")
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
//    val schema=schema_of_json("""{"type":"insert","timestamp":1576114094000,"databaseName":"aistrong","tableName":"test1","schema":"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"a","type":"long","nullable":true,"metadata":{}},{"name":"b","type":"long","nullable":true,"metadata":{}}]}","rows":[{"id":6,"a":1,"b":1}]}""")

    val data=Seq(

      Tuple1(3),
      Tuple1(2),
      Tuple1(4),
      Tuple1(5),
        Tuple1(1),
      Tuple1(1)
    )
//    val df: Dataset[lang.Long] = spark.range(100)
     val df=spark.createDataFrame(data).toDF("id")

    val df1=df.repartitionByRange(2,$"id")


    df.describe()
    df.summary()

    df.count()
    df.collect()
    df.head()
    df.first()

    //spark.sql.shuffle.partitions
//    val df1=df.repartition($"id")

    df1.withColumn("pid",spark_partition_id())
      .show(100)


//    val df1=df.rdd.mapPartitionsWithIndex((id,iter)=>{
////      iter.toList.foreach(println)
//      Iterator((id,iter.toList.size))
//  }).toDF("pid","psize")
//
//   df1.show()

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
