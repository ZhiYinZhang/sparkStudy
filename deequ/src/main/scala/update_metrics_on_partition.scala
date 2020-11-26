import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers.{Analysis, InMemoryStateProvider}
import com.amazon.deequ.checks.{Check, CheckLevel}
import org.apache.spark.sql.SparkSession

object update_metrics_on_partition {
  
  case class Manufacturer(id: Long, manufacturerName: String, countryCode: String)
  
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("deequ").master("local[2]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

//    在本例中，我们对按国家代码分区的制造商表进行操作
    val deManufacturers = Seq(
      Manufacturer(1, "ManufacturerA", "DE"),
      Manufacturer(2, "ManufacturerB", "DE")).toDF()

    val usManufacturers = Seq(
      Manufacturer(3, "ManufacturerD", "US"),
      Manufacturer(4, "ManufacturerE", "US"),
      Manufacturer(5, "ManufacturerF", "US")).toDF()

    val cnManufacturers = Seq(
      Manufacturer(6, "ManufacturerG", "CN"),
      Manufacturer(7, "ManufacturerH", "CN")).toDF()


    // 定义以下约束
    val check = Check(CheckLevel.Warning, "a check")
      .isComplete("manufacturerName")
      .containsURL("manufacturerName", _ == 0.0)
      .isContainedIn("countryCode", Array("DE", "US", "CN"))

    // Deequ now allows us to compute states for the metrics on which the constraints are defined according to the partitions of the data.

    //将Check转成Set(Analyzer)
    val analyzers = check.requiredAnalyzers().toSeq
    val analysis = Analysis(analyzers)



    // 第一步计算和存储每个分区的state
    val deStates = InMemoryStateProvider()
    val usStates = InMemoryStateProvider()
    val cnStates = InMemoryStateProvider()

    AnalysisRunner.run(deManufacturers, analysis, saveStatesWith = Some(deStates))
    AnalysisRunner.run(usManufacturers, analysis, saveStatesWith = Some(usStates))
    AnalysisRunner.run(cnManufacturers, analysis, saveStatesWith = Some(cnStates))

    // 接下来，我们根据分区状态计算整个表的度量
    // Note that we do not need to touch the data again, the states are sufficient
    val tableMetrics = AnalysisRunner.runOnAggregatedStates(deManufacturers.schema, analysis,
      Seq(deStates, usStates, cnStates))

    println("Metrics for the whole table:\n")
    tableMetrics.metricMap.foreach { case (analyzer, metric) =>
      println(s"\t$analyzer: ${metric.value.get}")
    }

    // Lets now assume that a single partition changes. We only need to recompute the state of this
    // partition in order to update the metrics for the whole table.

    val updatedUsManufacturers = Seq(
      Manufacturer(3, "ManufacturerDNew", "US"),
      Manufacturer(4, null, "US"),
      Manufacturer(5, "ManufacturerFNew http://clickme.com", "US")).toDF()

    // Recompute state of partition
    val updatedUsStates = InMemoryStateProvider()

    AnalysisRunner.run(updatedUsManufacturers, analysis, saveStatesWith = Some(updatedUsStates))

    // Recompute metrics for whole tables from states. We do not need to touch old data!
    val updatedTableMetrics = AnalysisRunner.runOnAggregatedStates(deManufacturers.schema, analysis,
      Seq(deStates, updatedUsStates, cnStates))

    println("Metrics for the whole table after updating the US partition:\n")
    updatedTableMetrics.metricMap.foreach { case (analyzer, metric) =>
      println(s"\t$analyzer: ${metric.value.get}")
    }
  }
}
