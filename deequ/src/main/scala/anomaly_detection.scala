import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.anomalydetection.SimpleThresholdStrategy
import com.amazon.deequ.checks.CheckStatus.Success
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import org.apache.spark.sql.SparkSession

/*
在这个简单的示例中，我们假设我们每天都要计算数据集的指标
确保它们不会发生剧烈变化。为了简单起见，我们只看
数据的大小
 */
object anomaly_detection {
  case class Item(id: Long, productName: String, description: String, priority: String, numViews: Long)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("deequ").master("local[2]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    //创建一个内存指标存储库
    val metricsRepository = new InMemoryMetricsRepository()

    //-----昨天数据的指标
    //指标的key，为了区分不同天的指标
//    val yesterdaysKey = ResultKey(System.currentTimeMillis() - 24 * 60 * 1000)
//    val yesterdaysDataset = Seq(
//      Item(1, "Thingy A", "awesome thing.", "high", 0),
//      Item(2, "Thingy B", "available at http://thingb.com", null, 0)).toDF()
//    /*
//    我们测试数据大小的异常，它不应该增加超过2倍。请注意我们将结果度量存储在存储库中
//     */
//    VerificationSuite()
//      .onData(yesterdaysDataset)
//      .useRepository(metricsRepository)
//      .saveOrAppendResult(yesterdaysKey)
//      .addAnomalyCheck(
//        RelativeRateOfChangeStrategy(maxRateIncrease = Some(2.0)), //异常检测的策略
//        Size()  //分析的值
//      )
//      .run()



    //-----今天的数据
    /*
    今天的数据有5行，所以数据大小增加了一倍以上，我们的异常检查应该抓住这
     */
//    val todaysDataset = Seq(
//      Item(1, "Thingy A", "awesome thing.", "high", 0),
//      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
//      Item(3, null, null, "low", 5),
//      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
//      Item(5, "Thingy E", null, "high", 12)).toDF()
//    val todaysKey = ResultKey(System.currentTimeMillis())
//
//    val verificationResult=VerificationSuite()
//      .onData(todaysDataset)
//      .useRepository(metricsRepository)
//      .saveOrAppendResult(todaysKey)
//      .addAnomalyCheck(
//        RelativeRateOfChangeStrategy(maxRateIncrease = Some(2.0)),
//        Size()
//      )
//      .run()
//
//    if(verificationResult.status!=Success){
//      metricsRepository.load()
//        .forAnalyzers(Seq(Size()))
//        .getSuccessMetricsAsDataFrame(spark)
//        .show()
//    }



    val todaysDataset = Seq(
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
      Item(3, null, null, "low", 5),
      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
      Item(5, "Thingy E", null, "high", 12)).toDF()
    val todaysKey = ResultKey(System.currentTimeMillis())

    val verificationResult=VerificationSuite()
      .onData(todaysDataset)
      .useRepository(metricsRepository)
      .saveOrAppendResult(todaysKey)
      .addAnomalyCheck(
        SimpleThresholdStrategy(upperBound=4),
        Size()
      )
      .run()


    if(verificationResult.status!=Success){
      metricsRepository.load()
        .forAnalyzers(Seq(Size()))
        .getSuccessMetricsAsDataFrame(spark)
        .show()
    }

  }
}