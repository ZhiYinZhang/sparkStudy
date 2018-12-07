package barrierScheduling

import org.apache.spark.BarrierTaskContext
import org.apache.spark.rdd.{RDD, RDDBarrier}
import org.apache.spark.sql.{Row, SparkSession}

object barrierDemo1 {
    def main(args:Array[String]):Unit={
      val spark: SparkSession = SparkSession.builder()
        .master("local[3]")
        .appName("barrierDemo1")
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      import spark.implicits._

      val df = spark.read
        .format("csv")
        .option("header",true)
        .option("inferSchema",true)
        .load("E:\\pythonProject\\dataset\\model01_train.csv")
      df.show(10)
      val a = df.rdd
      val a1: RDD[Row] = a.repartition(3)
      println(a1.partitions.length)
      val ba: RDDBarrier[Row] = a1.barrier()
      val result: RDD[Row] = ba.mapPartitions { iter =>
        println("start barrier")
        val context = BarrierTaskContext.get()
        if (context.partitionId() == 0) {
             println(context.attemptNumber())
             println(context.getTaskInfos())
             println(context.stageId())
             println(context.taskAttemptId())
             println(context.taskMetrics())
        }
        context.barrier()
        iter
      }
      result.collect()





    }
}
