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


    val delta_table="e://test//delta//test"

    //写一些初始数据到delta表
    val delta_df=spark.range(10).withColumn("value",spark_partition_id())
    delta_df.write.format("delta")save(delta_table)



    //用来更新delta表的数据
    val updateDF=spark.range(5,20).withColumn("value",spark_partition_id())


    val deltaTable: DeltaTable = DeltaTable.forPath(spark,delta_table)


    println("----------merge操作----------")
    deltaTable.as("events")
      .merge(updateDF.as("updates"),"events.id=updates.id")
      .whenMatched()
      .updateExpr(Map("id"->"updates.id"))
      .whenNotMatched()
      .insertExpr(Map("id"->"updates.id","value"->"updates.value"))
      .execute()

    deltaTable.history().show()
  }
}
