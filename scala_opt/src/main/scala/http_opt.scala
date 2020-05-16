import java.util

import scalaj.http._

import scala.collection.immutable
import scala.collection.immutable.NumericRange
import scala.collection.mutable.ListBuffer

object http_opt {
  def main(args: Array[String]): Unit = {

//    val files=List(390947040,390943379,390937896,390952833)

//    val files=List(71722846, 72059780, 71721622, 72058028, 72056791, 72060653, 72060029, 72062511, 72060487, 71725444)



  }

  def get_partitions(total_size:Long):Int={
  //total_size=total_size+file_num*4*1024*1024

    val defaultParallelism=200
    val bytesPerCore=total_size/defaultParallelism
    val bytesSplit=math.min(128*1024*1024,math.max(4*1024*1024,bytesPerCore))
    val p_num= math.ceil(total_size/bytesSplit).toInt
    println(s"defaultParallelism:${defaultParallelism},bytesPerCore:${bytesPerCore},"+
      s"bytesSplit:${bytesSplit},partitionNum:${p_num}")
    p_num
  }

  def percentile(list:Seq[String],quantiles:Array[Double]):Array[String]={
    val sorted = list.sorted

    for(i <- quantiles){
      require(i>=0 & i<=1,"quantile must be >=0 and 1<= ")
    }

    val length = sorted.length

    var result = Array[String]()

    for(quantile <- quantiles){
      val index = math.ceil(length*quantile).toInt-1
      result=result.:+(sorted(index))
    }
    result
  }

  def percentile1(list:Seq[Int],quantiles:Array[Double]):Array[Int]={
    val sorted = list.sorted

    for(i <- quantiles){
      require(i>=0 & i<=1,"quantile must be >=0 and 1<= ")
    }

    val length = sorted.length

    var result = Array[Int]()

    for(quantile <- quantiles){
      val index = math.ceil(length*quantile).toInt-1
      result=result.:+(sorted(index))
    }
    result
  }

}
