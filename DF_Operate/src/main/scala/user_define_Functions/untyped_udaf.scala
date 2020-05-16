package user_define_Functions

import java.util

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, count, expr, spark_partition_id, sum, udf, when}

import scala.collection.mutable.ListBuffer
object untyped_udaf {
   def main(args:Array[String]):Unit={
     //自定义聚合函数

      val spark=SparkSession.builder().appName("udaf")
       .master("local[2]")
       .getOrCreate()
     import spark.implicits._
     spark.sparkContext.setLogLevel("WARN")


     val df=spark.range(10)
       .repartition(3)
       .withColumn("pid",spark_partition_id())

     spark.udf.register("mySum",new mySum)
     spark.udf.register("myAverage",new myAvg)
     spark.udf.register("myPercentile",new percentile(Array(0.1,0.5,0.51)))


     df.show()

//     spark.sql("select group,mySum(id) as sum ,myAvg(id) as avg from myTable group by group").show()
//     df.groupBy("pid").agg(expr("myPercentile(id)")).show()

     df.selectExpr("myPercentile(id)").show()


//     val win = Window.partitionBy("pid")
//     df.withColumn("avg(id)",expr("myAverage(id)").over(win))
//       .show()


//     df.groupBy("group").agg(my_count("id")).show()

   }

  // 传进来的列 包含的是分组的数据   只是放到函数里面了
 def my_count(colName:String):Column={
   val result=count(col(colName))
   return result
 }
}




class mySum extends UserDefinedAggregateFunction{
  //输入数据的类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("str",DoubleType,true)))
  }
  // 中间进行聚合时，所处理的数据类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("count",DoubleType,true)))
  }
  //函数返回值的类型
  override def dataType: DataType = {
    DoubleType
  }

  override def deterministic: Boolean ={
    true
  }

  //为每个分组的数据执行初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit ={
    buffer(0)=0.0
  }
  //每个分组有新的值进来的时候,如何进行分组 对应的聚合值的计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getAs[Double](0)+input.getAs[Double](0)
  }
  //由于spark是分布式的，所以一个分组的数据，可能会在不同的节点上进行局部聚合，就是update
  //但是，最后一个分组，在各个节点上的聚合值，要进行merge，也就是合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getAs[Double](0)+buffer2.getAs[Double](0)
  }
  //一个分组的聚合值，如何通过中间的缓存聚合值，最后返回一个最终的聚合值
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Double](0)
  }
}



class myAvg extends UserDefinedAggregateFunction{
  //输入数据的类型
  override def inputSchema: StructType = {
    StructType(StructField("str",DoubleType,true)::Nil)
  }
  // 中间进行聚合时，所处理的数据类型
  override def bufferSchema: StructType = {
    //一列为 统计结果  一列为记录 个数
    StructType(StructField("sum",DoubleType,true)::StructField("count",DoubleType,true)::Nil)
  }
  //函数返回值的类型
  override def dataType: DataType = {
    DoubleType
  }

  override def deterministic: Boolean ={
    true
  }

  //为每个分组的数据执行初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit ={
    buffer(0)=0.0
    buffer(1)=0.0
  }
  //每个分组有新的值进来的时候,如何进行分组 对应的聚合值的计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getDouble(0)+input.getDouble(0)
    buffer(1)=buffer.getDouble(1)+1
  }
  //由于spark是分布式的，所以一个分组的数据，可能会在不同的节点上进行局部聚合，就是update
  //但是，最后一个分组，在各个节点上的聚合值，要进行merge，也就是合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getDouble(0)+buffer2.getDouble(0)
    buffer1(1)=buffer1.getDouble(1)+buffer2.getDouble(1)
  }
  //一个分组的聚合值，如何通过中间的缓存聚合值，最后返回一个最终的聚合值
  override def evaluate(buffer: Row): Double = {
    buffer.getDouble(0)/buffer.getDouble(1)
  }
}




class percentile(quantiles:Array[Double]) extends UserDefinedAggregateFunction{
  //输入数据的类型
  override def inputSchema: StructType = {
    StructType(StructField("str",DoubleType,true)::Nil)
  }
  // 中间进行聚合时，所处理的数据类型
  override def bufferSchema: StructType = {

    StructType(StructField("list",ArrayType(DoubleType),true)::Nil)
  }
  //函数返回值的类型
  override def dataType: DataType = {
    ArrayType(DoubleType)
  }

  override def deterministic: Boolean ={
    true
  }

  //为每个分组的数据执行初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit ={
    buffer(0)=Seq[Double]()
  }
  //每个分组有新的值进来的时候,如何进行分组 对应的聚合值的计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getSeq(0).:+(input.getDouble(0))
  }
  //由于spark是分布式的，所以一个分组的数据，可能会在不同的节点上进行局部聚合，就是update
  //但是，最后一个分组，在各个节点上的聚合值，要进行merge，也就是合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getSeq(0).++(buffer2.getSeq(0))
  }
  //一个分组的聚合值，如何通过中间的缓存聚合值，最后返回一个最终的聚合值
  override def evaluate(buffer: Row): Array[Double] = {
    percentile(buffer.getSeq(0),quantiles)
  }


  def percentile(list:Seq[Double],quantiles:Array[Double]):Array[Double]={
    val sorted = list.sorted

    for(i <- quantiles){
      require(i>=0 & i<=1,"quantile must be >=0 and 1<= ")
    }

    val length = sorted.length

    var result = Array[Double]()

    for(quantile <- quantiles){
      val index = math.ceil(length*quantile).toInt-1
      result=result.:+(sorted(index))
    }
    result
  }
}
