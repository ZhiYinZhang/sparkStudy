package delta_lake

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object update {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                          .master("local[*]")
                          .appName("delta update")
                          .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val df=spark.readStream.format("rate").load()

    val df1=df.withColumn("partition",when(col("value")/2===0,1)
                                           .when(col("value")/3===0,2)
        .when(col("value")/5===0,3).otherwise(4)
    )

    df1.withWatermark("timestamp","3 seconds")
      .writeStream.format("console").outputMode("append").start().awaitTermination()
//    val df2=df1.groupBy("partition").agg(sum("value"))
//    val df3=df1.groupBy("partition").agg(avg("value"))
//
//    df2.join(df3,"partition").writeStream.format("console")
//      .outputMode("complete").start().awaitTermination()

//    val deltaTable: DeltaTable = DeltaTable.forPath(spark,"e://test//delta//test1")
//
//
//    val updateDF=spark.range(5,15).withColumn("value",lit(10))
//
//
//    deltaTable.as("events")
//      .merge(updateDF.as("updates"),"events.id=updates.id")
//      .whenMatched
//      .updateExpr(Map("id"->"updates.id"))
//      .whenNotMatched
//      .insertExpr(Map("id"->"updates.id","value"->"updates.value"))
//      .execute()

  }
}
