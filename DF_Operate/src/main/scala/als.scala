import java.util

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.functions.{col, map}
import org.apache.spark.sql.types._

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer


object als {
  val param=Map("data_dir"->"E:\\test\\csv\\als\\part-00000.csv","result_dir"->"/home/zhangzy/dataset/als/result",
    "userCol"->"cust_id","itemCol"->"item_id","ratingCol"->"ratings",
    "top"->10)
  val data_dir=param("data_dir").toString
  val userCol=param("userCol").toString
  val itemCol=param("itemCol").toString
  val ratingCol=param("ratingCol").toString
  val top=param.getOrElse("top",10).toString.toInt

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder.appName("als").master("local[3]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
//    读取csv文件
    val df = spark.read.option("header",true).csv(data_dir)
        .withColumn(itemCol,col(itemCol).cast(LongType))
        .withColumn(ratingCol,col(ratingCol).cast(DoubleType))

    df.printSchema()
    //将 userCol StringIndex
    val userCol_int = param("userCol") + "_int"
    param("userCol_int") = userCol_int
    val df_int: DataFrame = string2index(df, userCol, userCol_int).withColumn(userCol_int,col(userCol_int).cast("int"))
//    df_int.printSchema()
//    df_int.show()

    //切分数据集
    val array: Array[Dataset[Row]] = df_int.randomSplit(Array(0.8, 0.2))
    var train = array(0)
    var test=array(1)

    //train
    val als: ALS = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol(param("userCol_int").toString)
      .setItemCol(itemCol).setRatingCol(ratingCol)
      .setColdStartStrategy("drop")
    val model: ALSModel = als.fit(train)

//    recForAllUser(train,model,param).show(false)
    recForAllItem1(train,model,param)
  }

  def recForAllItem1(df:DataFrame,model:ALSModel,param:Map[String,Any])={
    import df.sparkSession.implicits._
    val userCol_int=param("userCol_int").toString
    print("recommend for all item")
    /**     Generate top 10 user recommendations for each item   **/
    val itemRecs = model.recommendForAllItems(top)
    itemRecs.printSchema()
    itemRecs.show(false)

    /**    得到 userCol_int 与 userCol的映射     **/
    //去重
    val df_dup = df.dropDuplicates(userCol_int)
    //Map(userCol_int->userCol)
    val df_map=df_dup.select(map(col(userCol_int), col(userCol)))
    //拉取到driver
    val rows_map: Array[Row] = df_map.collect()
    //提取Map
    val tuples: Array[(Int, String)] = rows_map.flatMap(_.getMap[Int,String](0))
    //tupels转成Map
    val maps = Map.apply(tuples:_*)



//    val schema=new StructType().add("cust_id","string").add("rating","float")

//    val rowEncoder: ExpressionEncoder[Row] = RowEncoder(schema)
    //    implicit val list_row=Encoders.tuple(
    //                           Encoders.INT,
    //


//    Encoders.tuple(Encoders.INT,ArrayEncoder)
val schema1=new StructType()
  .add("item_id",IntegerType)
  .add("recommendations",ArrayType(StructType(Array(StructField("cust_id",StringType),StructField("rating",FloatType)))))
    implicit val encoder: ExpressionEncoder[Row] = RowEncoder(schema1)
    val result=itemRecs.map(r=>{
      val rows: util.List[Row] = r.getList[Row](1)
      val result: ListBuffer[Row] = ListBuffer[Row]()
      var row:Row=null
      //cust_id_int
      var key:Int= 0
      //cust_id
      var key_new:String=null
      var result_tuple:Row=null
      for(i <- 0 until rows.size()){
        row= rows.get(i)
        key=row.getInt(0)
        val key_new = maps(key)
        result_tuple=Row(key_new,row.getFloat(1))
        result.+=(result_tuple)
      }
      Row(r.getInt(0),result)
    }).toDF("item_id","recommendations")

    result.printSchema()
    result.show(false)
  }
  def recForAllUser(df:DataFrame,model:ALSModel,param:Map[String,Any]):DataFrame={
    val userCol_int=param("userCol_int").toString

    var userRecs: DataFrame = model.recommendForAllUsers(top)
    //将userCol_int 映射回来 userCol
    userRecs=userRecs.join(
      df.select(userCol, userCol_int)
        .dropDuplicates(userCol_int), userCol_int
    ).select(userCol, "recommendations")
    return userRecs
  }


  def recForAllItem(df:DataFrame,model:ALSModel,param:Map[String,Any])={
      import df.sparkSession.implicits._
      val userCol_int=param("userCol_int").toString
      print("recommend for all item")
/**     Generate top 10 user recommendations for each item   **/
      val itemRecs = model.recommendForAllItems(top)
      itemRecs.show(false)
      itemRecs.printSchema()

/**    得到 userCol_int 与 userCol的映射     **/
      //去重
      val df_dup = df.dropDuplicates(userCol_int)
      //Map(userCol_int->userCol)
     val df_map=df_dup.select(map(col(userCol_int), col(userCol)))
    //拉取到driver
    val rows_map: Array[Row] = df_map.collect()
    //提取Map
    val tuples: Array[(Int, String)] = rows_map.flatMap(_.getMap[Int,String](0))
    //tupels转成Map
    val maps = Map.apply(tuples:_*)

//    println(maps.keys)


    val schema=new StructType().add("cust_id","string").add("rating","float")
    val rowEncoder: ExpressionEncoder[Row] = RowEncoder(schema)
//    implicit val list_row=Encoders.tuple(
//                           Encoders.INT,
//    )

    val result=itemRecs.map(r=>{
          val rows: util.List[Row] = r.getList[Row](1)
          val result: ListBuffer[(String,Float)] = ListBuffer[(String,Float)]()
          var row:Row=null
          //cust_id_int
          var key:Int= 0
         //cust_id
          var key_new:String=null
          var result_tuple:(String,Float)=null
          for(i <- 0 until rows.size()){
              row= rows.get(i)
              key=row.getInt(0)
              key_new = maps(key)
              result_tuple=(key_new,row.getFloat(1))
              result.+=(result_tuple)
          }
         (r.getInt(0),result)
       }).toDF("item_id","recommendations")

    result.printSchema()
    result.show(false)
  }
  def string2index(df:DataFrame, inputCol:String, outputCol:String):DataFrame={
    print(s"string2index ${inputCol} to ${outputCol}")
    val stringIndexer=new StringIndexer().setInputCol(inputCol)
        .setOutputCol(outputCol)
    val model: StringIndexerModel = stringIndexer.fit(df)
    return model.transform(df)
  }

}
