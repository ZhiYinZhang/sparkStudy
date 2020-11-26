import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckResult}
import com.amazon.deequ.constraints.ConstraintResult
import org.apache.spark.sql.SparkSession

object basic {
  case class Item(id: Long, productName: String, description: String, priority: String, numViews: Long)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("deequ").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    val data = Seq(
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
      Item(3, null, null, "low", 5),
      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
      Item(5, "Thingy E", null, "high", 12)
    )

    val df = spark.createDataFrame(data)

    val vs=VerificationSuite()
      .onData(df)
      .addCheck(
        Check(CheckLevel.Error,"unit testing my data")
          .hasSize(_==5,Some("要求数量等于5"))//hint参数是写一些注释；要求数据行数有5行
          .isComplete("id",Some("要求id不能存在null值"))//要求id不能有null
          .isUnique("id",Some("要求id唯一"))//要求id唯一
          .isComplete("productName",Some("要求productName不能为null"))//要求productName不能有null
          .isContainedIn("priority",Array("high","low"))//要求只能包含high，low
          .isNonNegative("numViews")//要求numViews是非负数
          .containsURL("description",_>=0.5,Some("要求一半及以上的行包含url"))//要求一半即以上的行包含url
          .hasApproxQuantile("numViews",0.5,_<=10)
      ).run()

    //获取检查结果
    val results: Map[Check, CheckResult] = vs.checkResults


    results.flatMap(x=>{
      val results1: Seq[ConstraintResult] = x._2.constraintResults
      results1
    }).foreach(r=>{

      println(s"constraint:${r.constraint}")
      println(s"message:${r.message}")
      println(s"metric:${r.metric}")
      println(s"status:${r.status}")
      println("-"*100)
    })



  }
}
