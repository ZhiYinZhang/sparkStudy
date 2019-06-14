
import org.joda.time.DateTime
import scala.collection.mutable.Map

object opt_demo {
  def main(args: Array[String]): Unit = {
      val a1=Array("a","b","c")
      val a2=a1.dropWhile(x=>x.contains("a"))

      a1.foreach(println)
      a2.foreach(println)








  }
  val divider=(x:Double,y:Double)=>{
    var result: Double = 0
    try{
       result=(x/y).formatted("%.2f").toDouble
    }catch{
      case e:Exception=>{println(e.getMessage)}
    }
    result
  }
}