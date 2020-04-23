package user_define_Functions

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.{col, when}

object type_safe_udaf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("type safe udaf").getOrCreate()
    val df=spark.range(1000)


//    spark.udf.register("myAvg",new MyAverage)

//    val myAvg=new MyAverage

    val avg=MyAverage.toColumn.name("avg")

    df.select(avg).show()

  }

}

object MyAverage extends Aggregator[Int,(Double,Double),Double]{
  /**
    * 有三个变量：
    *    输入
    *    中间结果  因为我们要求均值，需要知道sum和count
    *    最终结果  最终结果等于sum/count
    * @return
    */
  //buffer的初始值
  override def zero: (Double, Double) = (0,0)


  override def reduce(b: (Double, Double), a: Int): (Double, Double) = {
    (b._1+a,b._2+1)
  }
  //合并两个buffer
  override def merge(b1: (Double, Double), b2: (Double, Double)): (Double, Double) = {
    (b1._1+b2._1,b1._2+b2._2)
  }

  override def finish(reduction: (Double, Double)): Double = {
    //计算均值
    reduction._1/reduction._2
  }

  override def bufferEncoder: Encoder[(Double, Double)] = Encoders.tuple(Encoders.scalaDouble,Encoders.scalaDouble)

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

