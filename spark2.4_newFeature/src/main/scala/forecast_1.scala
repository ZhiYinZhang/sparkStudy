
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object forecast_1 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("forecast_sales_custid")
      .master("local[1]")
      .config("spark.memory.faction",0.8)
      .getOrCreate()
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val filePath="E:\\test\\csv\\forecast.csv"

    var df= spark.read
      .option("header",true)
      .csv(filePath)
    df=df.withColumn("qty_sum",$"qty_sum".cast("double"))
        .withColumn("amt_sum",$"amt_sum".cast("double"))
    df.show(truncate=false)
    df.printSchema()



    val train_df=num_preprocess(df)

    val rollings_qty_sum=get_lag_rolling_feature(train_df,"qty_sum",List(5,10),List(5,10))
//    rollings_qty_sum.write.option("header",true).csv("e://test/csv/result1")
    getJVM()
    println(rollings_qty_sum.count())
//    val features=(for{x<-rollings_qty_sum.columns if x.startsWith("qty_sum_lag")}yield x).toArray
//    val forecast_qty_sum = new forecast_per_cust_id_model(rollings_qty_sum,features,"qty_sum",new LinearRegression())
//
//    val pred = forecast_qty_sum.get_forecasting(rollings_qty_sum)
//    pred.select("cust_id","born_date","qty_sum","prediction").show()
//    pred.write.csv("E:\\test\\csv\\result1")
  }

  def getJVM(): Unit ={
    val runtime: Runtime = Runtime.getRuntime
    println("max:"+runtime.maxMemory()/1024/1024)
    println("total:"+runtime.totalMemory()/1024/1024)
    println("free:"+runtime.freeMemory()/1024/1024)
  }
  def num_preprocess(df:DataFrame):DataFrame={
      var df1=df.groupBy("cust_id","born_date")
                 .agg(("qty_sum","sum"),("amt_sum","sum"))
      df1=df1.withColumn("born_date",to_date(df1("born_date"),"yyyyMMdd"))
             .withColumnRenamed("sum(qty_sum)","qty_sum")
             .withColumnRenamed("sum(amt_sum","amt_sum")
      df1=df1.orderBy("cust_id","born_date")
      df1
  }

  /**
    * feature engineering
    */


  val get_max=udf((arr:Array[Double])=>arr.max)
  val get_min=udf((arr:Array[Double])=>arr.min)
  val get_sum=udf((arr:Array[Double])=>arr.sum)
//  val get_mean=udf((arr:Array[Double])=>arr.sum/arr.size)

  private val features: Map[String, UserDefinedFunction] = Map("min"->get_min,"sum"->get_sum,"max"->get_max)
  def get_lag_rolling_feature(df:DataFrame,col:String,lags:List[Int],windows:List[Int],features:Map[String,UserDefinedFunction]=features):DataFrame={
        var window_s=windows.sorted
        var lag_s=lags.sorted
        var i=0
    var loop1=0
        var rollings:DataFrame=null
        for(l<-lag_s){
            println("l:"+l)
            var temp=df.alias("temp")
            temp = temp.withColumn("born_date",date_sub(temp("born_date"),l))
            var j=1

            for(window<-window_s){
                println("window:"+window)

                for(win <- Range(j,window+1)){
                    println("loop1:"+loop1)
                    loop1+=1
                    System.gc()
                    var lag=temp.alias("lag")
                    lag = lag.withColumn("born_date", date_sub(lag("born_date"),win)).select("cust_id","born_date")
                    lag = lag.join(temp.select("cust_id","born_date",col),List("cust_id","born_date"),"left")
                    lag = lag.withColumnRenamed(col,col+"_"+win+"_lag")
                    lag = lag.withColumn("born_date", date_add(lag("born_date"), win))
                    temp = temp.join(lag, List("cust_id","born_date"))
                    j = win+1
                    temp = temp.na.fill(0)
                }
               // create rolling feature one by one
               var loop2=0
               for(name<-features.keys) {
                 println("loop2:" + loop2)
                 loop2 += 1
                 val c = (for {column <- temp.columns if column.endsWith("_lag")} yield temp(column)).toList
                 temp = temp.withColumn(col + "_lag_" + l + "_rolling_" + name + "_" + window, features(name)(array(c: _*)))
                 println("loop2 stop")
               }
            }
            //join the rolling features of lag l together
            temp = temp.withColumn("born_bate",date_add(temp("born_date"),l))
            println("row 101")
            if(i==0){
              println("i="+i)
              val select_col=List("cust_id","born_date","qty_sum")++(for{x<-temp.columns if x.startsWith("qty_sum_lag_")}yield x)
              rollings=temp.select(select_col.map(temp(_)):_*).alias("rollings")
              i += 1
            }else{
              val select_col=List("cust_id","born_date")++(for{x<-temp.columns if x.startsWith("qty_sum_lag_")}yield x)
              rollings=rollings.join(temp.select(select_col.map(temp(_)):_*),List("cust_id","born_date"))
            }
//           println("cache start")
//            rollings.cache()
        }
        rollings=rollings.na.fill(0)
        rollings=rollings.orderBy("cust_id","born_date")
        println("get_lag_rolling_feature stop")
        return rollings
  }


}
/**
  * predict
  */
class forecast_per_cust_id_model(df:DataFrame,feature:Array[String],target:String,var model:LinearRegression){
     def build_vector(): DataFrame ={
       println("start build_vector")
        val vectorAssembler =  new VectorAssembler()
             .setInputCols(feature)
             .setOutputCol("features")
         var train = vectorAssembler.transform(df)
         train = train.select("features",target)
       println("build_vecotr end")
         return train
     }
  def train_model():LinearRegressionModel={
    println("start vectorassembling")
    var train = build_vector()
//    train.cache()
    println("start building model")
    model = model.setFeaturesCol("features").setLabelCol(target)
    println("start training")
    val trained_model = model.fit(train)
    println("finish training")
    return trained_model
  }


  def get_forecasting(testset:DataFrame): DataFrame ={
    println("start get_forecasting")
    val trained_model = train_model()
    val vectorAssembler=new VectorAssembler().setInputCols(feature).setOutputCol("features")
    val testset1 = vectorAssembler.transform(testset)
    println("start forecasting")
    val pred = trained_model.transform(testset1)
    println("finish forecasting")
    pred.printSchema()
    pred.select("qty_sum","prediction").show()
    return pred
  }

}
