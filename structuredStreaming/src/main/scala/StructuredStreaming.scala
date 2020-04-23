import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType

object StructuredStreaming {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .appName("structStreaming")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cols = Map("3month_hangout"-> "integer",
      "4g_unhealth"-> "integer",
      "6month_avg_comsume"-> "double",
      "age"-> "integer",
      "contacts"-> "double",
      "current_amount"-> "double",
      "current_left"-> "double",
      "fax_app"-> "double",
      "finance_app"-> "double",
      "fly_app"-> "double",
      "hangout"-> "integer",
      "in_debt"-> "integer",
      "integerernet_age"-> "double",
      "is_black_list"-> "integer",
      "is_gym"-> "integer",
      "is_identity"-> "integer",
      "is_movie"-> "integer",
      "is_sam"-> "integer",
      "is_student"-> "integer",
      "is_tour"-> "integer",
      "is_wanda"-> "integer",
      "last_pay_amount"-> "double",
      "last_pay_time"-> "integer",
      "pur_app"-> "double",
      "target"-> "double",
      "tours_app"-> "double",
      "train_app"-> "double",
      "user_id"-> "string",
      "vedio_app"-> "double",
      "fee_sen_0"-> "integer",
      "fee_sen_1"-> "integer",
      "fee_sen_2"-> "integer",
      "fee_sen_3"-> "integer",
      "fee_sen_4"-> "integer",
      "fee_sen_5"-> "integer",
      "is_age_true"-> "integer",
      "total_app"-> "double")

    var tp = new StructType()
    for(col <- cols){
      tp=tp.add(col._1,col._2)
    }

    val schema: StructType = spark.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("e://test//csv")
      .schema



    val df1 = spark.readStream
      .format("csv")
      .option("header",true)
      .option("seq",",")
      .schema(schema)
      .load("e://test//csv")


    df1.printSchema()
//
//
    val query: StreamingQuery = df1.writeStream
      .outputMode("append")
      .format("console")
      .start()
    query.awaitTermination()


  }
}
