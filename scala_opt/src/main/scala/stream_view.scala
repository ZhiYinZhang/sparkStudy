import scala.util.Random

/**
  *    Stream:一个惰性加载的List
  *    它只确定第一个值，后面的值
  */
object stream_view {
  def main(args: Array[String]): Unit = {
    val l: List[Int] = (1 to 10000000).map(_=>Random.nextInt(10000)).toList
    val start0=System.currentTimeMillis()

    l.filter(_%3==0).take(10)

    val end0=System.currentTimeMillis()

    l.toStream.filter(_%3==0).take(10)
    val end1=System.currentTimeMillis()

    println(end0-start0,end1-end0)
  }
}
