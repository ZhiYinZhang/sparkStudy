import scala.runtime.Nothing$

//companion object
object scala_basis {
  def main(args: Array[String]): Unit = {

    println(factorial(3))
    println(sumRange(1, 100))
    println(sumSquares(1, 3))

    val m1=Map("runoob"->"www.runoob.com","google"->"www.google.com")
     new scala_basis().show1()
  }

  def show(x:Option[String])=x match{
      //Some(s)说明有值
    case  Some(s)=>s
    case None=>"?"
  }

  //求连续整数的平方和
  def sumSquares(a:Int,b:Int):Int={
    if(a>b)
      0
    else
      a*a+sumSquares(a+1,b)
  }
  //求给定两个数区间中的所有整数的和  a<b
  def sumRange(a:Int,b:Int):Int={
    if(a>b)
      0
    else
      a+sumRange(a+1,b)
  }
  //求阶乘
  def factorial(num:Int): Int ={
     if(num<=0)
       1
    else
       num*factorial(num-1)
  }
}
//companion class
class scala_basis{
  private def show1()={
    println("companion class")
  }
}
//private修饰的只能本类里面使用，加了[a]限定，表示除了a包下面，其他包不能用
package a{
  package b{
    private[a] class c{
      protected[b] def d(){println("d")}
      class e{
        private[c] val f=100
        private[this] var g=200
      }
    }
  }
  class h{
    import b._

  }

}