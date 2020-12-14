import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers.{Analysis, InMemoryStateProvider}
import com.amazon.deequ.checks.{Check, CheckLevel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 测试只有这一个环境的，没有其他无关的依赖
 */
object only_env {
	def main (args: Array[String]): Unit = {
		val spark=SparkSession.builder().appName("deequ").master("local[2]").getOrCreate()
		spark.sparkContext.setLogLevel("warn")

		val rate=spark.readStream.format("rate")
				.option("rowsPerSecond",1)
				.load()
				.toDF()

		//定义Check
		val check=Check(CheckLevel.Error,"check stream data")
				.hasSize(_>=0)
				.isComplete("timestamp",Some("要求不能存在null值"))
				.isComplete("value")
				.isUnique("value",Some("要求唯一"))
		//将Check转成Analyzer分析器集合
		val analyzers=check.requiredAnalyzers().toSeq
		//构建Analysis
		val analysis=Analysis(analyzers)

		val stateStore=InMemoryStateProvider()


		val query=rate.writeStream.foreachBatch{
			(df:DataFrame,batchId:Long)=>{
				println(s"batchId:$batchId")
				df.show()
				if(batchId==0){
					val analyzerContext=AnalysisRunner.run(data=df,analysis=analysis,saveStatesWith=Some(stateStore))
					analyzerContext.metricMap.foreach{case (analyzer,metric)=>println(s"\t$analyzer:${metric.value.get}")}
				}else{
					val analyzerContext=AnalysisRunner.run(
						data=df,
						analysis=analysis,
						saveStatesWith=Some(stateStore),
						aggregateWith=Some(stateStore))
					analyzerContext.metricMap.foreach{case (analyzer,metric)=>println(s"\t$analyzer:${metric.value.get}")}

				}

			}

		}.start()

		query.awaitTermination()
	}
}
