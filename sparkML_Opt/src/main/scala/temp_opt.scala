import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession

object temp_opt {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("ml")
      .master("local[3]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("WARN")

    val dataset=List(("a",1),("b",2),("c",3),("a",4))
    val df=spark.createDataFrame(dataset).toDF("user","value")

    val value =new  StringIndexer().setInputCol("user").setOutputCol("user_int")
    val model = value.fit(df)
    val labels = model.labels


    val df1=model.transform(df)

    df1.show()

    val index2String = new IndexToString().setInputCol("user_int").setOutputCol("user2").setLabels(Array("12e","34f","01g"))
    val df2 = index2String.transform(df1)

    df2.show()
  }
}
