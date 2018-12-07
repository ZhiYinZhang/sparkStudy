package barrierScheduling

import org.apache.spark.sql.SparkSession

object barrierDemo1 {
    def main(args:Array[String]):Unit={
      val spark: SparkSession = SparkSession.builder()
        .master("local[3]")
        .appName("barrierDemo1")
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      import spark.implicits._

      





    }
}
