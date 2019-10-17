

import java.lang

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.util.Random
import scala.collection.mutable.Map

case class SimpleData(name:String,value:Double)
object demo1 {
  def main(args: Array[String]): Unit = {
      val spark=SparkSession.builder()
      .appName("test")
      .master("local[2]")
        .config("spark.sql.autoBroadcastJoinThreshold",80*1024*1024)
      .getOrCreate()
      import spark.implicits._
      spark.sparkContext.setLogLevel("warn")

      spark.range(10).withColumn("value",lit("abc 中文"))
        .withColumn("hash",hash(col("value")))
      .withColumn("md5",md5(col("value")))
      .withColumn("sha1",sha1(col("value")))
      .withColumn("sha2",sha2(col("value"),256))
        .show(truncate=false)

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
