import com.microsoft.ml.spark.LightGBMRegressor
import org.apache.spark.sql.SparkSession

object mmlspark_lgb {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("lgb")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val filePath = "e://test//csv"
    val df1 = spark.read
    .option("header",true)
    .option("inferSchema",true)
      .csv(filePath)
    val lgb = new LightGBMRegressor()
    lgb.setAlpha(0.3)
      .setLearningRate(0.3)
      .setNumIterations(100)
      .setNumLeaves(333)
        .setFeaturesCol("4g_unhealth")

    lgb.fit(df1)

  }
}
