package mapOrFlatMap_Groups

import java.sql.Timestamp

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{KeyValueGroupedDataset, Row, SparkSession}
import org.apache.spark.sql.functions._
object flatMapGroups {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("flatMapGroups").master("local[3]").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

   //city,date,order
    val df=spark.read.format("delta").load("e://test//delta//test")

    val df1=df.withColumn("date",date_trunc("month",$"date"))
    df1.printSchema()



    val groupDF: KeyValueGroupedDataset[String, Row] = df1.groupByKey(row=>row.getString(0))


    val groupDF1 = df1.groupByKey(row=>row.getString(0))
    //将两个dataFrame的相同key的值聚合在一起
    groupDF.cogroup(groupDF1)(cogroup)


    //mapValues(func:(V)=>W)：函数的参数为 groupByKey的value，即Row,返回的类型还是一个KeyValueGroupedDataset
//    groupDF.mapValues(_.getLong(2)).reduceGroups(_+_).show()

//flatMapGroups：这个可以实现和Window.partitionBy().orderBy类似的功能
//    val result=groupDF.flatMapGroups(topN).toDF("city","date","total_order")

  }
  def topN(key:String,value:Iterator[Row]): Iterator[(String,Timestamp,Long)] ={
    val values=value.toSeq
    //先将Row -> (date,order),然后根据date分组
    val timestampToTuples: Map[Timestamp, Seq[(Timestamp, Long)]] = values.map(v => (v.getTimestamp(1), v.getLong(2)))
      .groupBy(_._1)

    //将相同date的order进行sum
    val timestampToLong: Map[Timestamp, Long] = timestampToTuples.map(v => {
      val sum = v._2.map(_._2).sum
      (v._1, sum)
    })

    //根据统计值倒序排序
    val tuples: Seq[(String,Timestamp, Long)] = timestampToLong.toSeq.sortWith(_._2>_._2).take(3).map(v=>(key,v._1,v._2))
    tuples.toIterator
  }

  def cogroup(key:String,value:Iterator[Row],value1:Iterator[Row]):Iterator[String]={
    Iterator("")
  }
}
