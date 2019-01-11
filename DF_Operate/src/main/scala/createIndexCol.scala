import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StructType
/*
给DataFrame新增一列递增的id列
*/
object createIndexCol {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("garbage classfication")
      .master("local[5]")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("WARN")


   var df1: DataFrame = spark.range(0,11).toDF("col1")
    var df2: DataFrame = spark.range(5,16).toDF("col2")
//    println(System.currentTimeMillis())
//    println(df1.rdd.getNumPartitions,df2.rdd.getNumPartitions)
// 将两个DataFrame并在一起
//    val tempRDD: RDD[(Row, Row)] = df1.rdd.zip(df2.rdd)
//    println(tempRDD.getNumPartitions)
////    tempRDD.foreach(println(_))
////
//    val tempRDD1 = tempRDD.map(x=>{
//      Row(x._1.get(0),x._2.get(0))
//    })
//    val schema= new StructType().add("col1","long").add("col2","long")
//    spark.createDataFrame(tempRDD1,schema).select($"*",spark_partition_id()).show()
//    print(df1.rdd.getNumPartitions)

     //1.生成单调递增，不保证连续，64bit的一列.分区数不变
//    df1 = df1.withColumn("index",monotonically_increasing_id())

    //2.生成按某列排序后，添加的单调递增，连续的一列。DataFrame分区数变为1
//    df1 = df1.withColumn("index",row_number().over(Window.orderBy("col1")))

    //3.将DataFrame转成RDD，使用RDD的方法zipWithIndex()/zipWithUniqueId()，分区数不变
//    var tempRDD: RDD[(Row, Long)] = df1.rdd.zipWithIndex()
//    println(System.currentTimeMillis())
//    val record: RDD[Row] = tempRDD.map(x => {
//          Row(x._1.get(0), x._2)
//        })
//    println(System.currentTimeMillis())
//    val schema= new StructType().add("value","long")
//            .add("id","long")
//    val df2 = spark.createDataFrame(record,schema)

//    4.使用map遍历
//    val start = System.currentTimeMillis()
//    df1 = df1.repartition(1)
//     var id = 0
//   df1.map(x => {
//      val t = (x.getLong(0),id)
//      id += 1
//      t
//    }).show()
//
//    val end = System.currentTimeMillis()
//    println(end-start)




  }
}
