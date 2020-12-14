import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers.{Analysis, InMemoryStateProvider}
import com.amazon.deequ.checks.{Check, CheckLevel}
import org.apache.spark.sql.{DataFrame, SparkSession}

object stream_metric {
  case class demo(id:Long,value:String)
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().appName("deequ").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    //定义Check
    val check=Check(CheckLevel.Error,"check stream data")
      .hasSize(_>=0)
      .isComplete("value",Some("要求不能存在null值"))
      .isUnique("value",Some("要求唯一"))
    //将Check转成Analyzer分析器集合
    val analyzers=check.requiredAnalyzers().toSeq
    //构建Analysis
    val analysis=Analysis(analyzers)

    //定义state存储
    val stateStore=InMemoryStateProvider()


    val rate=spark.readStream.format("rate")
      .option("rowsPerSecond",1)
      .load()

    val query=rate.writeStream.foreachBatch{
      (df:DataFrame,batchId:Long)=>{
        println(s"batchId:$batchId")
        df.show()
        println(s"count:${df.count()}")
        if(!df.isEmpty){//df不能为空
          val analyzerContext = AnalysisRunner.run(
            data = df,
            analysis = analysis,
            aggregateWith = Some(stateStore),//和历史state聚合
            saveStatesWith = Some(stateStore)//将结果state存回
          )
          analyzerContext.metricMap.foreach{
            case (analyzer,metric)=>
              println(s"\t$analyzer:${metric.value.get}")
          }
        }

      }
    }.start()

    query.awaitTermination()
  }
}
