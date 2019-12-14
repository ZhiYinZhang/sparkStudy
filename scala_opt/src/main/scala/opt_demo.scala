


import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.joda.time.DateTime

import org.joda.time.{DateTime, Days}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.collection.mutable.Map
import scala.collection._
object opt_demo {
  def main(args: Array[String]): Unit = {

    val m=Map("2019-09-01"->1,"2019-11-01"->2,"2019-10-01"->1,"2019-12-01"->4)

    val d=m.map(v=>(DateTime.parse(v._1),v._2))

    val r=d.map(v=>{
      val date=v._1
//      val value=v._2

      val lastMonth=date.minusMonths(1)
//      val lastValue=d.getOrElse(lastMonth,0)

      val value=1
      val lastValue=0
      val a:Double=try{(value-lastValue)/lastValue}catch{
        case ex:Exception=>{Double.NaN}
      }
      (date,a)
    })

    println(r)


    val dtf=DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

    println(DateTime.parse("2019-10-13 10:10:10", dtf))



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