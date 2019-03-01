package com.entrobus.withSparkML

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.util.matching.Regex

object predict_rent {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    //数据源目录
    //     val inputPath = "E:\\test\\csv\\smart"
    val outputPath = args(1)
    //val outputPath ="e://test//csv//result

    //初始化spark
    val spark = SparkSession.builder()
      .appName("smart_city")
      .master("local[3]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    //广播SparkSession实例
    val bd = sc.broadcast[SparkSession](spark)

    import spark.implicits._

    //数据的schema
    val params = List(
      ("rent_id","string"),
      ("address", "string"),
      ("rent", "string"),
      ("rent_type", "string"),
      ("area", "string"),
      ("decorate", "string"),
      ("longitude", "string"),
      ("latitude", "string"),
      ("county_id", "string"))
    var schema = new StructType()
    for(param <- params){
      schema = schema.add(param._1,param._2)
    }

    //创建流DataFrame
    var df = spark.readStream
      .format("csv")
      .option("header",true)
      .schema(schema)
      .load(inputPath)
    df.printSchema()

    df = df.repartition(1)

    val value = fit_static_df(df,bd,outputPath)

    value.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }
  def fit_static_df(df:DataFrame,bd:Broadcast[SparkSession],outputPath:String):DataFrame={
    var model:LinearRegressionModel = null
    //自定义Encoder
    implicit val myEncoder = org.apache.spark.sql.Encoders.kryo[Row]
    val value: Dataset[Row] = df.mapPartitions(iterators => {
      var l: List[(String, String, String, String, String, String, String, String, String)] = List[(String, String, String, String, String, String, String, String, String)]()
      for (ite <- iterators) {
        if(ite.getString(2)!=null && ite.getString(3)!=null && ite.getString(4)!=null && ite.getString(5)!=null) {
          val tuple: (String, String, String, String, String, String, String, String, String) = (ite.getString(0), ite.getString(1), ite.getString(2), ite.getString(3), ite.getString(4), ite.getString(5), ite.getString(6), ite.getString(7), ite.getString(8))
          l = l.+:(tuple)
        }
      }

      val spark1: SparkSession = SparkSession.builder().appName("static_df").master("local[3]").getOrCreate()
      //创建静态DataFrame
      val df: DataFrame = spark1.createDataFrame(l).toDF("rent_id","address","rent","rent_type","area","decorate","longitude","latitude","county_id")

      //预处理
      val df1 = df.withColumn("rent_type_int",when(col("rent_type")==="整租",1).otherwise(0))
        .withColumn("decorate_int",when(col("decorate")==="豪华装修" or col("decorate")==="精装修",1).otherwise(0))
        .withColumn("rent_int",regexp_extract(col("rent"),"\\d+",0).cast("integer"))
        .withColumn("area_int",regexp_extract(col("area"),"\\d+",0).cast("integer"))

      model = build_linear_regression(df1)
      var pred = get_rent_prediction(df1, model)

      pred = pred.drop("rent_int","rent_type_int","area_int","decorate_int","feature","prediction")
      pred =  pred.withColumn("round",concat(col("round"),regexp_replace(col("rent"),"\\d+","")))


      pred = pred.repartition(1)
      pred.write
        .mode("overwrite")
        .format("json")
        .option("header",true)
        .save(outputPath)

      iterators
    })

    value
  }


  def clean_rent(string:String):Int={
    val regex: Regex = new Regex("\\d+")
    val maybeString: Option[String] = regex.findFirstIn(string)
    maybeString.get.toInt
  }
  def preprocess_rent(df:DataFrame, f:(String)=>Int):DataFrame={
    ////
    //df: 待处理dataframe//f
    //处理函数
    ////
    val udf_onehot_rent_type = udf(f)
    return df.withColumn("rent", udf_onehot_rent_type(df("rent") ) )
  }
  def onehot_decorate(x:String):Int={
    if(x == "豪华装修" || x == "精装修"){
      return 1
    }else
      return 0

  }
  def onehot_rent_type(x:String):Int={
    if(x == "整租"){
      return 1
    }else
      return 0
  }


  def preprocess_rent_type(df:DataFrame, f:(String)=>Int):DataFrame={
    ////
    //df: 待处理dataframe//f
    //处理函数
    ////
    val udf_onehot_rent_type = udf(f)
    return df.withColumn("rent_type", udf_onehot_rent_type(df("rent_type") ) )

  }

  def preprocess_decorate(df:DataFrame, f:(String)=>Int):DataFrame={
    ////
    // df: 待处理dataframe
    // f: 处理函数
    ////
    val udf_onehot_decorate = udf(f)
    return df.withColumn("decorate", udf_onehot_decorate(df("decorate")))

  }

  def clean_area(string:String):Int={
    val regex: Regex = new Regex("\\d+")
    val maybeString: Option[String] = regex.findFirstIn(string)
    maybeString.get.toInt
  }


  def preprocess_area(df:DataFrame,f:(String)=>Int):DataFrame={
    ////
    // df: 待处理dataframe
    // f: 处理函数
    ////
    val udf_clean_area = udf(f)
    return df.withColumn("area", udf_clean_area(df("area")))

  }

  def build_linear_regression(df:DataFrame):LinearRegressionModel={
    ////
    // df: 训练集
    ////

    val vectorAssembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("rent_type_int", "decorate_int", "area_int"))
      .setOutputCol("feature")
    var train: DataFrame = vectorAssembler.transform(df)
    train = train.select("feature","rent_int")
    val lr = new LinearRegression()
      .setFeaturesCol("feature")
      .setLabelCol("rent_int")
      .setMaxIter(10)
    val model: LinearRegressionModel = lr.fit(train)
    return model

  }
  def round_num(x:Column, num:Int=0):Column={
    return round(x, num)
  }

  def get_rent_prediction(df:DataFrame, model:LinearRegressionModel):DataFrame={
    ////
    // df: 测试集

    val vectorAssembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("rent_type_int", "decorate_int", "area_int"))
      .setOutputCol("feature")
    var test: DataFrame = vectorAssembler.transform(df)

    val pred: DataFrame = model.transform(test)

    return pred.withColumn("round",round(pred("prediction"),0))
  }
}
