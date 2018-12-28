
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostEstimator}
object loan_risk {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
    .appName("loanRisk")
    .master("local[2]")
    .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val file_path = "E:\\资料\\ml\\ML-At-Scale-eBook-181029\\Lending_Club_Loan_Data\\lending-club-loan-data\\loan.csv"

    var loan_stats = spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(file_path)

    //预处理
    //将loan_status不在["Default","Charged Off","Fully Paid"]中的过滤，
   loan_stats = loan_stats.filter(
        //isin(list:Any*)传入可变参
        loan_stats("loan_status").isin("Default","Charged Off","Fully Paid")
    ).withColumn(
      "bad_loan",
      (!(loan_stats("loan_status")==="Fully Paid")).cast("string")
    )
//    loan_stats.groupBy("addr_state").agg((count(col("annual_inc"))).alias("ratio")).show()
//    val emp_length = loan_stats.select(trim(regexp_replace($"emp_length","<|\\+|(years)|(year)","")).cast("int"))

    loan_stats = loan_stats.withColumn(
      "emp_length",trim(regexp_replace($"emp_length","<|\\+|(years)|(year)","")).cast("double"))
      .withColumn("annual_inc",$"annual_inc".cast("double"))
      .withColumn("dti",$"dti".cast("double"))
      .withColumn("delinq_2yrs",$"delinq_2yrs".cast("double"))
      .withColumn("revol_util",$"revol_util".cast("double"))
      .withColumn("total_acc",$"total_acc".cast("double"))

    loan_stats.printSchema()



    val dataset: Array[Dataset[Row]] = loan_stats.randomSplit(Array(0.7,0.3))
    val dataset_valid = dataset(1).cache()
    val dataset_train = dataset(0).cache()

    // Define our categorical and numeric columns
    val categoricals = Array("term", "home_ownership", "purpose", "addr_state","verification_status","application_type")
    //                        double       string       string    string     string     string        string
    //    val numerics = Array("loan_amnt","emp_length", "annual_inc","dti","delinq_2yrs","revol_util","total_acc","credit_length_in_years")
    val numerics = Array("loan_amnt","emp_length", "annual_inc","dti","delinq_2yrs","revol_util","total_acc")


    // Imputation estimator for completing missing values
    val numerics_out = numerics.map(_+"_out")
    val imputers = new Imputer()
      .setInputCols(numerics)
      .setOutputCols(numerics_out)

    // Apply StringIndexer for our categorical data
    val categoricals_idx = categoricals.map(_+"_idx")
    val indexers = categoricals.map(
      x=>new StringIndexer().setInputCol(x).setOutputCol(x+"_idx").setHandleInvalid("keep")
    )

    // Apply OHE for our StringIndexed categorical data
    val categoricals_class = categoricals.map(_+"_class")
    val oneHotEncoders = new OneHotEncoderEstimator()
      .setInputCols(categoricals_idx)
      .setOutputCols(categoricals_class)

    //Set feature columns
    val featureCols = categoricals_class ++ numerics_out

    //Create assembler for our numeric columns (including label)
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    //Establish label
    val labelIndexer = new StringIndexer()
      .setInputCol("bad_loan")
      .setOutputCol("label")

    //Apply StandardScaler
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithMean(true)
      .setWithStd(true)

    // Build pipeline array
    val pipelineAry = indexers ++ Array(oneHotEncoders,imputers,assembler,labelIndexer,scaler)

    //
    // Train XGBoost Model with training dataset
    //

    // Create XGBoostEstimator

    val xgBoostEstimator = new XGBoostEstimator(
      Map[String, Any](
        "num_round" -> 5,
        "objective" -> "binary:logistic",
        "nworkers" -> 16,
        "nthreads" -> 4
      )
    ) .setFeaturesCol("scaledFeatures")
      .setLabelCol("label")

    // Create XGBoost Pipeline
    val xgBoostPipeline = new Pipeline().setStages(pipelineAry ++ Array(xgBoostEstimator))

    // Create XGBoost Model based on the training dataset
    val xgBoostModel = xgBoostPipeline.fit(dataset_train)

    val predictions: DataFrame = xgBoostModel.transform(dataset_valid)

    predictions.printSchema()
    predictions.show()
  }
}
