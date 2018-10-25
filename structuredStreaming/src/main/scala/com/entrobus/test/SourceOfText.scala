package test

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SourceOfText {
  /*def updataFunc(key:UserId,newValues:Iterator[Event],state:GroupState[Int]):Int={
    val totalEvents=state.get()+newValues.size()
    state.update(totalEvents)
    state.setTimeoutDuration("30 min")
    return totalEvents
  }*/
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .appName("text")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val lineDF: DataFrame = spark.readStream
      .format("text")
      .load("E:/test/txt")
    val wordDS: Dataset[String] = lineDF.as[String].flatMap(_.split(" "))


    /*val wordAndOne: Dataset[(String, Int)] = wordDS.map((_,1))
    wordAndOne.groupByKey(_._1).mapGroupsWithState(updataFunc)*/



     val resultDF: DataFrame = wordDS.groupBy("value").count()


    val query:StreamingQuery=resultDF.writeStream
      .outputMode("complete")
      .format("console")
//      .option("checkpointLocation","e:\\Entrobus\\checkpoint")
//      .option("path","e:\\Entrobus\\output4")
      .start()

    query.awaitTermination()


  }
}
