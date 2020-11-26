import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers._
import org.apache.spark.sql.SparkSession

object increment_metric {
  case class Item(id: Long, productName: String, description: String, priority: String, numViews: Long)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("deequ").master("local[2]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")


    val data =Seq(
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available tomorrow", "low", 0),
      Item(3, "Thing C", null, null, 5)
    ).toDF()

    val moreData = Seq(
      Item(4, "Thingy D", null, "low", 10),
      Item(5, "Thingy E", null, "high", 12),
      Item(6, "Thingy E", null, "high", 5)
    ).toDF()


    val analysis = Analysis()
      .addAnalyzer(Size())
      .addAnalyzer(ApproxCountDistinct("id"))
      .addAnalyzer(Completeness("productName"))
      .addAnalyzer(Completeness("description"))
      .addAnalyzer(Size(Some("numViews>=5")))

    val stateStore = InMemoryStateProvider()


    val metricsForData = AnalysisRunner.run(
      data = data,
      analysis = analysis,
      //注意：这里是赋值给saveStateWith
      saveStatesWith = Some(stateStore) // persist the internal state of the computation
    )


    // We update the metrics now from the stored states without having to access the previous data!
    val metricsAfterAddingMoreData: AnalyzerContext = AnalysisRunner.run(
      data = moreData,
      analysis = analysis,
      //这里是赋值给aggregateWith
      aggregateWith = Some(stateStore) // continue from internal state of the computation
    )


    println("Metrics for the first 3 records:\n")
    metricsForData.metricMap.foreach { case (analyzer, metric) =>
      println(s"\t$analyzer: ${metric.value.get}")
    }

    println("\nMetrics after adding 2 more records:\n")
    metricsAfterAddingMoreData.metricMap.foreach { case (analyzer, metric) =>
      println(s"\t$analyzer: ${metric.value.get}")
    }
  }
}
