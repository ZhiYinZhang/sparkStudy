
import scala.collection.mutable.Map
object opt_demo {
  def main(args: Array[String]): Unit = {
      val l=List(1,2,3,4,5)

    println(l.map(x => x + 1))
      println(l.map{x=>x+2})



  }
}