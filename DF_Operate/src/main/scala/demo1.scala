

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._


case class SimpleData(name:String,value:Double)
object demo1 {
  def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .appName("demo")
          .master("local[5]")
          .config("spark.ui.port","36000")
    //      .config("spark.speculation",true)
    //      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()
        val sc = spark.sparkContext
        import spark.implicits._
        sc.setLogLevel("WARN")



        var df=spark.createDataFrame(List(
          (1,2,3,4),
          (2,3,4,5),
          (3,4,5,6),
          (4,5,6,7)
        )).toDF(List("a","b","c","d"):_*)
        df.select(least("a","b")).show()

       for(c <- df.columns)
                df=df.withColumnRenamed(c,c+"1")
       df.show()
//          val arr=spark.createDataFrame(Seq(
//            Array(1,2,3,Array(4,5,6)),
//            Array(2,3,4,5,Array(5,5,5)),
//            Array(3,4,5,6)
//
//          ).map(Tuple1.apply(_))).toDF("arr")
//         arr.select(explode($"arr")).show()


//     Thread.sleep(10*60*1000)
  }
}
