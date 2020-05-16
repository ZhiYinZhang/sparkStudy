package user_define_Functions

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._

case class Employee(name:String,salary:Long)
case class Average(var sum:Double,var count:Long)

object type_safe_udaf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("type safe udaf").getOrCreate()
    val ds: Dataset[Employee] = spark.range(1000).withColumnRenamed("id", "salary")
      .withColumn("name", lit("a")).as[Employee](Encoders.product)


    val avg=MyAverage.toColumn.name("avg")
    ds.select(avg).show()

  }

}

object MyAverage extends Aggregator[Employee,Average,Double]{
  /**
    * 有三个变量：
    *    输入
    *    中间结果  因为我们要求均值，需要知道sum和count
    *    最终结果  最终结果等于sum/count
 *
    * @return
    */
  override def zero: Average = Average(0,0)

  override def reduce(b: Average, a: Employee): Average = {
    b.sum+=a.salary
    b.count+=1
    b
  }

  override def merge(b1: Average, b2: Average): Average = {
    b1.sum+=b2.sum
    b1.count+=b2.count
    b1
  }

  override def finish(reduction: Average): Double = reduction.sum/reduction.count

  override def bufferEncoder: Encoder[Average] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

