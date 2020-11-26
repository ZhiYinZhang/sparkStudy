import com.amazon.deequ.suggestions.{ConstraintSuggestion, ConstraintSuggestionRunner, Rules}
import org.apache.spark.sql.SparkSession

object constraint_suggestion {
  case class RawData(productName: String, totalNumber: String, status: String, valuable: String)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("deequ").master("local[2]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val rows =Seq(
      RawData("thingA", "13.0", "IN_TRANSIT", "true"),
      RawData("thingA", "5", "DELAYED", "false"),
      RawData("thingB", null, "DELAYED", null),
      RawData("thingC", null, "IN_TRANSIT", "false"),
      RawData("thingD", "1.0", "DELAYED", "true"),
      RawData("thingC", "7.0", "UNKNOWN", null),
      RawData("thingC", "24", "UNKNOWN", null),
      RawData("thingE", "20", "DELAYED", "false"),
      RawData("thingA", "13.0", "IN_TRANSIT", "true"),
      RawData("thingA", "5", "DELAYED", "false"),
      RawData("thingB", null, "DELAYED", null),
      RawData("thingC", null, "IN_TRANSIT", "false"),
      RawData("thingD", "1.0", "DELAYED", "true"),
      RawData("thingC", "17.0", "UNKNOWN", null),
      RawData("thingC", "22", "UNKNOWN", null),
      RawData("thingE", "23", "DELAYED", "false")
    ).toDF()

    /*
    我们要求deequ为我们计算数据上的约束建议;它将分析数据并应用addConstraintRules()中指定的一组规则
去建议约束
     */
    val suggestionResult = ConstraintSuggestionRunner()
      .onData(rows)
      .addConstraintRules(Rules.DEFAULT)
      .run()
    /*
    我们现在可以研究deequ建议的约束条件。对于每个建议的约束，我们会得到一个文本描述和相应的scala代码。
    注意，约束建议是基于启发式规则的，并假设显示的数据是“静态的”和正确的，这在现实世界中可能并不常见。
    因此，在将建议应用于实际部署之前，应该手动检查这些建议。
     */
    //key为列名，value为约束建议
    val col_suggestions: Map[String, Seq[ConstraintSuggestion]] = suggestionResult.constraintSuggestions

    col_suggestions.foreach{case (column,suggestions)=>
        println(column)
        //每一列有多个建议
        var num=0
        suggestions.foreach{
          suggestion=>
            num+=1
            println(s"suggestion${num}:")
            println(suggestion.description)
            println(suggestion.codeForConstraint)
            println("-"*100)
        }
       println("#"*100)
    }


  }
}
