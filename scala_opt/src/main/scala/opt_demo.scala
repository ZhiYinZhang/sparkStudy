
import org.joda.time.DateTime
import scala.collection.mutable.Map

object opt_demo {
  def main(args: Array[String]): Unit = {
      val l=List(1,2,4,5,6,7,8,9)








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