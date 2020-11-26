import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.Completeness
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import org.apache.spark.sql.SparkSession

object metrics_repo {
  case class Item(id: Long, productName: String, description: String, priority: String, numViews: Long)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("deequ").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // The toy data on which we will compute metrics
    val data = Seq(
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
      Item(3, null, null, "low", 5),
      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
      Item(5, "Thingy E", null, "high", 12))
    val df=spark.createDataFrame(data)


    // The repository which we will use to stored and load computed metrics; we use the local disk,
    // but it also supports HDFS and S3
    val repository: MetricsRepository =
    FileSystemMetricsRepository(spark, "e://data//deequ//metrics.json")



    // The key under which we store the results, needs a timestamp and supports arbitrary
    // tags in the form of key-value pairs
    val resultKey = ResultKey(System.currentTimeMillis(), Map("tag" -> "repositoryExample"))

    VerificationSuite()
      .onData(df)
      // Some integrity checks
      .addCheck(Check(CheckLevel.Error, "integrity checks")
        .hasSize(_ == 5)
        .isComplete("id")
        .isComplete("productName")
        .isContainedIn("priority", Array("high", "low"))
        .isNonNegative("numViews"))
      // We want to store the computed metrics for the checks in our repository
      .useRepository(repository)
      .saveOrAppendResult(resultKey)
      .run()

    // We can now retrieve the metrics from the repository in different ways, e.g.:


    //我们可以ResultKey查询
    val completenessOfProductName = repository
      .loadByKey(resultKey).get
      .metric(Completeness("productName")).get

    println(s"The completeness of the productName column is: $completenessOfProductName")

    //我们可以查询存储库中最近10分钟的所有指标，并以json的形式获得它们
    val json = repository.load()
      .after(System.currentTimeMillis() - 10000)
      .getSuccessMetricsAsJson()

    println(s"Metrics from the last 10 minutes:\n$json")

    //我们还可以根据标记值进行查询，并以dataframe的形式检索结果
    repository.load()
      .withTagValues(Map("tag" -> "repositoryExample"))
      .getSuccessMetricsAsDataFrame(spark)
      .show()

    //以json返回所有metrics
    repository.load().getSuccessMetricsAsJson()
    repository.load().getSuccessMetricsAsJson(withTags = Seq("repositoryExample"))
    //以dataframe返回所有metrics
    repository.load().getSuccessMetricsAsDataFrame(spark).show()
    repository.load().getSuccessMetricsAsDataFrame(spark,withTags = Seq("repositoryExample")).show()
  }
}
