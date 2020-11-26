import com.amazon.deequ.profiles.{ColumnProfile, ColumnProfilerRunner, NumericColumnProfile}
import org.apache.spark.sql.SparkSession

object data_profiling {

  case class RawData(productName: String, totalNumber: String, status: String, valuable: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("deequ").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    val rawData = Seq(
      RawData("thingA", "13.0", "IN_TRANSIT", "true"),
      RawData("thingA", "5", "DELAYED", "false"),
      RawData("thingB", null, "DELAYED", null),
      RawData("thingC", null, "IN_TRANSIT", "false"),
      RawData("thingD", "1.0", "DELAYED", "true"),
      RawData("thingC", "7.0", "UNKNOWN", null),
      RawData("thingC", "20", "UNKNOWN", null),
      RawData("thingE", "20", "DELAYED", "false")
    )

    val df = spark.createDataFrame(rawData)

    val result = ColumnProfilerRunner()
      .onData(df)
      .run()

    //key为列名，value为列的分析
    val profiles: Map[String, ColumnProfile] = result.profiles

    profiles.foreach {
      case (c, profile) =>
        println(profile.dataType)
        println(profile.completeness)
        println(profile.column)
        //是否进行数据推断
        println(profile.isDataTypeInferred)
        //每列推断的各类型数量
        println(profile.typeCounts)
        //每列的值的种类，各类的数量及占比
        println(profile.histogram)
        //去重后的数量
        println(profile.approximateNumDistinctValues)
        println("-" * 100)
    }



    //对数字类型的列，还可以做其他的分析
    //如对数字列totalNumber
    val totalNumberProfile = profiles("totalNumber").asInstanceOf[NumericColumnProfile]
    println(totalNumberProfile.maximum.get)
    println(totalNumberProfile.minimum.get)
    println(totalNumberProfile.mean.get)
    println(totalNumberProfile.stdDev.get)
  }
}
