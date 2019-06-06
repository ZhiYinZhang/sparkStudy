import scala.util.matching.Regex

object regex_opt {
  def main(args: Array[String]): Unit = {
    val regex: Regex = new Regex("cit(y|ies)")

    val str="city cities county"


    //必须使用mkString
    println(regex.findAllIn(str).mkString(","))
  }
}
