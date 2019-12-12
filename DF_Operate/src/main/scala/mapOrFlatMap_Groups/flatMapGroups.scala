package mapOrFlatMap_Groups

import org.apache.spark.sql.SparkSession

object flatMapGroups {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("topN").master("local[3]").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
  }
}
