package barrierScheduling

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object barrierDemo1 {
    def main(args:Array[String]):Unit={
      val spark: SparkSession = SparkSession.builder()
        .master("local[3]")
        .appName("barrierDemo1")
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      import spark.implicits._

      val filePath="D:\\PycharmProjects\\DPT\\files\\dataset\\model01_train.csv"
      val df1: DataFrame = spark.read
        .format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load(filePath)

      df1.printSchema()
//      val col1: Column = df1("mnth")
      val arr = Array("col1","col2","col3","col4")
      df1.select(
        when($"instant"<1000,0)
          .when($"instant"<2000,1)
          .when($"instant"<3000,2)
          .otherwise(3).as("when")
      ).groupBy("when").count().show()



      val win: WindowSpec = Window.partitionBy("mnth")
        .orderBy($"mnth".asc)
        .rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
      df1.select($"mnth",sum("instant").over(win)).show()


      df1.select($"instant".between(1,100)).show()
    }
}
