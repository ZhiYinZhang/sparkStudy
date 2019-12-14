import java.util.Date

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerBlockManagerAdded, SparkListenerBlockUpdated, SparkListenerEnvironmentUpdate, SparkListenerExecutorAdded, SparkListenerExecutorMetricsUpdate, SparkListenerExecutorRemoved, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart, StageInfo}
import org.apache.spark.storage.BlockManagerId

class mySparkListener extends SparkListener with Logging {
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    //这个是在sparkContext启动时调用，但是sc.addSparkListener是在sparkContext创建之后
    //必须通过config配置spark.extraListeners参数，传入类名
    val appName = applicationStart.appName
    logInfo(f"******************$appName application start****************")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val time = new Date(applicationEnd.time)

    println(f"******************application end $time******************")
  }



  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    val executorInfo = executorAdded.executorInfo
    println("**********************executor add************************")
    println(f"executor id:${executorAdded.executorId}")
    println(f"executor host:${executorInfo.executorHost}")
    println(f"executor total cores:${executorInfo.totalCores}")
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    println("***********************executor metrics update**********************")
    println(f"executor id:${executorMetricsUpdate.execId}")
    val accumUpdates=executorMetricsUpdate.accumUpdates


    println(accumUpdates.size)
//    println(f"accum updates:${executorMetricsUpdate.accumUpdates}")


  }
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    println("********************executor remove***********************")
    println(f"executor id:${executorRemoved.executorId}")
    println(f"executor reason:${executorRemoved.reason}")

  }

  //  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
  //    val details = environmentUpdate.environmentDetails
  //    println("enviroment info:")
  //    details.foreach(x=>println(x._1,x._2.mkString(",")))
  //  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    println("**************************block manager added********************")
    val blockManagerId: BlockManagerId = blockManagerAdded.blockManagerId

    println(f"blockManager id:${blockManagerAdded.blockManagerId}")
    println(f"blockManager host:${blockManagerId.host}")
    println(f"blockManager executorId:${blockManagerId.executorId}")
    println(f"block manager maxMem:${blockManagerAdded.maxMem}")
    println(f"block manager maxOffHeapMem:${blockManagerAdded.maxOffHeapMem}")
    println(f"block manager maxOnHeapMem:${blockManagerAdded.maxOnHeapMem}")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println("********************job start****************")
    println(f"job id:${jobStart.jobId}")
    println(f"stage ids:${jobStart.stageIds.mkString(",")}")
  }
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    println("*********************stage submitted***********************")
    val stageInfo: StageInfo = stageSubmitted.stageInfo
    println(f"stage name:${stageInfo.name}")
    println(f"stage id:${stageInfo.stageId}")
    println(f"stage parentIds:${stageInfo.parentIds.mkString(",")}")
    println(f"task Num:${stageInfo.numTasks}")

  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println("************************stage completed*********************")
    val stageInfo = stageCompleted.stageInfo
    val metrics = stageInfo.taskMetrics
    println(f"disk bytes spilled:${metrics.diskBytesSpilled}")
    println(f"executor cpu time:${metrics.executorCpuTime}")
    println(f"executor deserializer cpu time:${metrics.executorDeserializeCpuTime}")
    println(f"executor deserializer  time:${metrics.executorDeserializeTime}")
    println(f"executor run time:${metrics.executorRunTime}")
    println(f"jvm gc time:${metrics.jvmGCTime}")
    println(f"memory bytes spilled:${metrics.memoryBytesSpilled}")
    println(f"peak execution meomry:${metrics.peakExecutionMemory}")
    println(f"result serialization time:${metrics.resultSerializationTime}")
    println(f"result size:${metrics.resultSize}")

    println(f"input metrics bytes read:${metrics.inputMetrics.bytesRead}")
    println(f"input metrics records read:${metrics.inputMetrics.recordsRead}")
    println(f"output metrics bytes written:${metrics.outputMetrics.bytesWritten}")
    println(f"output metrics records written:${metrics.outputMetrics.recordsWritten}")


    val shuffleReadMetrics = metrics.shuffleReadMetrics
    val shuffleWriteMetrics = metrics.shuffleWriteMetrics
    println(f"shuffle read fetch wait time:${shuffleReadMetrics.fetchWaitTime}")
    println(f"shuffle read records:${shuffleReadMetrics.recordsRead}")
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    println("****************task start********************")
    val taskInfo = taskStart.taskInfo

    println(f"stage attempt id:${taskStart.stageAttemptId}")
    println(f"stage id:${taskStart.stageId}")
    println(f"task id:${taskInfo.taskId}")
    println(f"attempt number:${taskInfo.attemptNumber}")
    println(f"launch time:${taskInfo.launchTime}")
    println(f"speculative:${taskInfo.speculative}")
    println(f"task locality:${taskInfo.taskLocality}")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    println("****************task end********************")
    val taskInfo = taskEnd.taskInfo
    println(f"task duration:${taskInfo.duration}")
    println(f"finish time:${taskInfo.finishTime}")
  }
}

