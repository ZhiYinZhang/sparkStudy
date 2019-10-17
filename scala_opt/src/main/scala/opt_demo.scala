
import org.joda.time.DateTime
import scala.collection.mutable.Map

object opt_demo {
  def main(args: Array[String]): Unit = {
    val ints = (0 to 10).toArray
    ints.update(0,100)

    ints.foreach(println)


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
  def printfor(x:Int):Unit={
    (0 to x).foreach(println)
  }
}
class opt_demo1{
  def printfor(x:Int):Unit={
    (0 to x).foreach(println)
  }
}