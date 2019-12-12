


import java.sql.Timestamp
import java.util.Date

import org.joda.time.DateTime

import scala.collection.mutable.Map
import scala.collection._
object opt_demo {
  def main(args: Array[String]): Unit = {

    val t=new Timestamp(new Date().getTime)
    println(t)
   val i=Iterator(1,2,3,4)
    println(i.size)
    println(i.size)



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