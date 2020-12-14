import scala.util.Random

object demo1 {
  def main(args: Array[String]): Unit = {
        val a=1
        val b=2
        println(s"${a},${b}")

       new Random().nextInt()
  }
}

/**
 * private:只允许私有成员在定义了该成员的类或对象的内部可见，同样的规则还适用内部类
 */
class demo1{
  class Inner{
    private def f1(){println("f1")}
    def f2(){println("f2")}
    class InnerMost{
      f1()
    }
  }
  //new Inner().f1()//错误
  new Inner().f2()
}

/**
 *protected:只允许保护成员在定义了该成员的类的子类中被访问。
 */
package p{
  class Super{
    protected def f(){println("f")}
  }
  class Sub extends Super{
    f()
  }
  class Other{
    //new Super().f()//错误
  }
}

class Outer {
  class Inner {
    def f() { println("f") }
    class InnerMost {
      f() // 正确
    }
  }
  (new Inner).f() // 正确因为 f() 是 public
}

package bobsrockets{
  package navigation{
    private[bobsrockets] class Navigator{
      protected[navigation] def useStarChart(){}
      class LegOfJourney{
        private[Navigator] val distance = 100
      }
      private[this] var speed = 200
    }
  }
  package launch{
    import navigation._
    object Vehicle{
      private[launch] val guide = new Navigator
    }
  }
}