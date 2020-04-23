package windowOperator

import org.apache.spark.SparkExecutorInfo
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, StreamingQueryProgress, Trigger}

object waterMark {
 def main(args:Array[String]):Unit={
    val spark:SparkSession=SparkSession.builder()
      .appName("waterMark")
     .config("spark.driver.extraJavaOptions","-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC")
      .config("spark.executor.extraJavaOptions","-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC")
      .master("local[3]")
      .getOrCreate()

   import spark.implicits._
   val sc=spark.sparkContext
   sc.setLogLevel("WARN")
   
   val lines = spark.readStream
                   .format("rate")
                   .option("rowsPerSecond",1)
                   .load()





   val df1=lines.withWatermark("timestamp","5 minutes")

   val df2=df1.groupBy(window($"timestamp","10 minutes","10 minutes"))
     .agg(sum($"value"))

//1.append:在这个模式中，只有过期不再更新的窗口才会 append到Result Table / Sink(在console模式下，只有过期的窗口才会打印出来)
   val query: StreamingQuery = df2.writeStream
     .outputMode("update")
     .format("console")
     .option("truncate", false)
     .trigger(Trigger.ProcessingTime("1 minute"))
     .start()


   while(true){
     val infos: Array[SparkExecutorInfo] = sc.statusTracker.getExecutorInfos
     for(executorInfo<-infos){
       val totalOnHeap = executorInfo.totalOnHeapStorageMemory()
       val totalOffHeap = executorInfo.totalOffHeapStorageMemory()
       val usedOnHeap = executorInfo.usedOnHeapStorageMemory()
       val usedOffHeap = executorInfo.usedOffHeapStorageMemory()

       println(s"totalOnHeap:$totalOnHeap,totalOffHeap:$totalOffHeap,usedOnHeap:$usedOnHeap,usedOffHeap:$usedOffHeap")
     }

         val totalMemory=Runtime.getRuntime.totalMemory()
         val maxMemory=Runtime.getRuntime.maxMemory()
         println(s"totalMemory:$totalMemory,maxMemory:$maxMemory")
         Thread.sleep(1000*10)
   }
 }

}
