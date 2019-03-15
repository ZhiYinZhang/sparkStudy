
import org.json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
object json_opt {
  def main(args: Array[String]): Unit = {
    val j_str="""{"int":1,"d":1.02,"arr":[1,2,3],"map":{}}"""
    val value: JValue = parse(j_str)

    println(value.pascalizeKeys)
    println(value.snakizeKeys)
    println(value.underscoreKeys)



    //  ~  为org.json4s.JsonDSL.~
//    val jObject: json4s.JObject = ("name" -> "joe") ~ ("age" -> 35)
//    compact(render(jObject))
//    println(pretty(render(jObject)))
  }
}
