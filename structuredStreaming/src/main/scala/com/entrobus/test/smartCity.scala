package com.entrobus.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object smartCity {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
    .appName("smart")
    .master("local[3]")
    .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val params = List(
      ("rent_id","string"),
      ("location","string"),
      ("address", "string"),
      ("rent", "string"),
      ("rent_type", "string"),
      ("facing","string"),
      ("apt_type","string"),
      ("floor","string"),
      ("area", "string"),
      ("decorate", "string"),
      ("longitude", "string"),
      ("latitude", "string"),
      ("adcode", "string"))
    var schema = new StructType()
    for(param <- params){
      schema = schema.add(param._1,param._2)
    }

    val filePath = "E:\\test\\csv\\smart"

    val stream0 = spark.readStream
    .format("csv")
    .option("header","true")
    .schema(schema)
    .load(filePath)

    var static0 = spark.createDataFrame(Seq(
      ("fs_348020867_20190130","距广佛线岗站约231米千灯湖南海中海寰宇天下","桂城桂澜路𧒽岗地铁站上盖","5000元/月（押一付二","整租","南北","3室2厅2卫","高层","129平米","精装修","nan","nan","nan")
    )).toDF("rent_id","location","address","rent","rent_type","facing","apt_type","floor","area","decorate","longitude","latitude","adcode")


//
//    df0.withColumn("rent_type_int",when(col("rent_type")==="整租",1).otherwise(0))
//      .withColumn("decorate_int",when(col("decorate")==="豪华装修" or col("decorate")==="精装修",1).otherwise(0))
//      .withColumn("rent_int",regexp_extract($"rent","\\d+",0).cast("integer"))
//      .withColumn("area_int",regexp_extract($"area","\\d+",0).cast("integer")).printSchema()

//    df0.withColumn("pred",concat($"area",regexp_replace($"rent","\\d+",""))).show()


    implicit val myEncoder = org.apache.spark.sql.Encoders.kryo[Row]
    val value: Dataset[Row] = stream0.mapPartitions(iterators => {
      var l: List[(String, String, String, String, String, String, String, String, String,String, String, String, String)] = List[(String, String, String, String, String, String, String, String, String,String, String, String, String)]()
      for (ite <- iterators) {
        if(ite.getString(3)!=null && ite.getString(4)!=null && ite.getString(8)!=null && ite.getString(9)!=null) {
          val tuple: (String, String, String, String, String, String, String, String, String,String, String, String, String) = (ite.getString(0), ite.getString(1), ite.getString(2), ite.getString(3), ite.getString(4), ite.getString(5), ite.getString(6), ite.getString(7), ite.getString(8), ite.getString(9), ite.getString(10), ite.getString(11), ite.getString(12))
          l = l.+:(tuple)
        }
      }

      val spark1: SparkSession = SparkSession.builder().appName("static_df").master("local[2]").getOrCreate()

      //创建静态DataFrame
      val df: DataFrame = spark1.createDataFrame(l).toDF("rent_id","location","address","rent","rent_type","facing","apt_type","floor","area","decorate","longitude","latitude","adcode")
      val rdd: RDD[Row] = df.rdd
      spark1.createDataFrame(rdd,schema)


      static0 = static0.union(df)
      static0.write.format("csv").save("e://test//csv//smart//stream")
      iterators
    }
      )

    stream0.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }


}
