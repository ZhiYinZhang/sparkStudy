import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers._
import org.apache.spark.sql.{DataFrame, SparkSession}

object stream_metric2 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("deequ").master("local[2]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")


    val analysis: Analysis = Analysis()
      .addAnalyzer(Size())
      .addAnalyzer(Completeness("value"))
      .addAnalyzer(Uniqueness("id"))

    val stateProvider = InMemoryStateProvider()



    val socket=spark.readStream.format("socket")
      .option("host","202.198.129.43")
      .option("port","9999")
      .load()
      .map{
        r=>
          val cols=r.getString(0).split(",")
          (cols(0),cols(1))
      }.toDF("id","value")

    val query=socket.writeStream.foreachBatch{
      (df:DataFrame,batchId:Long)=>{
        println(s"batchId:$batchId")
        if(!df.isEmpty){
          val analyzerContext = AnalysisRunner.run(data = df, analysis = analysis, aggregateWith = Some(stateProvider), saveStatesWith = Some(stateProvider))
          analyzerContext.metricMap.foreach{
            case (analyzer,metric)=>
              println(s"\tanalyzer:$analyzer\n\tmetricName:${metric.name}\n\tmetricValue:${metric.value.get}\n\t${"-"*100}")
          }
        }
      }
    }.start()

      query.awaitTermination()

  }
}
