import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.Duration
object foreach_batch {
       def main(args:Array[String]):Unit={
         val spark: SparkSession = SparkSession.builder()
           .appName("foreachBatch")
           .master("local[2]")
           .getOrCreate()
         spark.sparkContext.setLogLevel("WARN")
         import spark.implicits._

         val df1: DataFrame = spark.readStream
           .format("rate")
           .option("rowsPerSecond",5)
           .option("rampUpTime",3) //初始化好了，再等多久产生数据
           .load()
         val result: DataFrame = df1.selectExpr("value % 10 as key")
         .groupBy("key")
         .count()
         .toDF("key","value")


         val query: StreamingQuery = df1.writeStream
             .foreachBatch{
               (batchDF:DataFrame,batchId:Long)=>{
                 batchDF.persist()
                 batchDF.write.format("csv").mode("append").option("header",true).save("e://test//first1")
                 batchDF.write.format("json").mode("append").save("e://test//first2")
                 batchDF.unpersist()
               }
             }
//           .format("console")
           .option("truncate",false)
           .trigger(Trigger.ProcessingTime(1000))
           .start()

         query.awaitTermination()




       }
}
