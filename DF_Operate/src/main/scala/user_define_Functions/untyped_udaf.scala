package user_define_Functions

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, count, sum, udf, when}
object untyped_udaf {
   def main(args:Array[String]):Unit={
     //自定义聚合函数

      val spark=SparkSession.builder().appName("udaf")
       .master("local[2]")
       .getOrCreate()
     import spark.implicits._
     spark.sparkContext.setLogLevel("WARN")


     val df=spark.range(1000)
       .withColumn("group",when(col("id")%2===0,1)
                                       .when(col("id")%7===0,2)
                                       .when(col("id")%13===0,3)
                                       .otherwise(4)
       )

     spark.udf.register("mySum",new mySum)
     spark.udf.register("myAvg",new myAvg)
     df.createOrReplaceTempView("myTable")


//     spark.sql("select group,mySum(id) as sum ,myAvg(id) as avg from myTable group by group").show()

     df.selectExpr("mySum(id)").show()



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
    StructType(Array(StructField("str",IntegerType,true)))
  }
  // 中间进行聚合时，所处理的数据类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("count",IntegerType,true)))
  }
  //函数返回值的类型
  override def dataType: DataType = {
    IntegerType
  }

  override def deterministic: Boolean ={
    true
  }

  //为每个分组的数据执行初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit ={
    buffer(0)=0
  }
  //每个分组有新的值进来的时候,如何进行分组 对应的聚合值的计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getAs[Int](0)+input.getAs[Int](0)
  }
  //由于spark是分布式的，所以一个分组的数据，可能会在不同的节点上进行局部聚合，就是update
  //但是，最后一个分组，在各个节点上的聚合值，要进行merge，也就是合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getAs[Int](0)+buffer2.getAs[Int](0)
  }
  //一个分组的聚合值，如何通过中间的缓存聚合值，最后返回一个最终的聚合值
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}




class myAvg extends UserDefinedAggregateFunction{
  //输入数据的类型
  override def inputSchema: StructType = {
    StructType(StructField("str",IntegerType,true)::Nil)
  }
  // 中间进行聚合时，所处理的数据类型
  override def bufferSchema: StructType = {
    //一列为 统计结果  一列为记录 个数
    StructType(StructField("sum",IntegerType,true)::StructField("count",IntegerType,true)::Nil)
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
    buffer(0)=0
    buffer(1)=0
  }
  //每个分组有新的值进来的时候,如何进行分组 对应的聚合值的计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getInt(0)+input.getInt(0)
    buffer(1)=buffer.getInt(1)+1
  }
  //由于spark是分布式的，所以一个分组的数据，可能会在不同的节点上进行局部聚合，就是update
  //但是，最后一个分组，在各个节点上的聚合值，要进行merge，也就是合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getInt(0)+buffer2.getInt(0)
    buffer1(1)=buffer1.getInt(1)+buffer2.getInt(1)
  }
  //一个分组的聚合值，如何通过中间的缓存聚合值，最后返回一个最终的聚合值
  override def evaluate(buffer: Row): Double = {
    buffer.getInt(0)/buffer.getInt(1)
  }
}