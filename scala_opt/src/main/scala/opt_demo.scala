
import scala.collection.mutable.Map
object opt_demo {
  def main(args: Array[String]): Unit = {
    val x=1.1
    val y=2
    val rt=divider(x,y)
    println(rt)
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