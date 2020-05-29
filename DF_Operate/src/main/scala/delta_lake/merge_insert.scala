package delta_lake

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object merge_insert {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("delta update")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")


    val delta_table="e://test//delta//test"

    val init_df=spark.range(10)
      .withColumn("value",lit("a"))
      .withColumn("dt",current_date())
    init_df.write.format("delta").partitionBy("dt").mode("append").save(delta_table)


    val origin=DeltaTable.forPath(delta_table)

    val update_df=spark.range(5,10)
      .withColumn("value",lit("b"))
      .withColumn("dt",current_date())


    origin.alias("origin")
      .merge(update_df.alias("update"),"origin.id=update.id")
      .whenMatched()
      .updateExpr(Map("value"->"update.value"))
      .whenNotMatched()
      //必须指定origin里面所有列的值
//      .insertExpr(Map("id"->"update.id","value"->"update.value")) //报错
      .insertExpr(Map("id"->"update.id","value"->"update.value","dt"->"update.dt"))
      .execute()
  }
}
