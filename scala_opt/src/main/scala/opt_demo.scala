
import org.joda.time.DateTime

import scala.collection.mutable.Map
object opt_demo {
  def main(args: Array[String]): Unit = {
    val dt = new DateTime()
    println(dt.toString())
    println(dt.toString("yyyy-MM-dd HH:mm:ss.SSS"))
    val dt0=dt.plusDays(10)
    println(dt0)


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