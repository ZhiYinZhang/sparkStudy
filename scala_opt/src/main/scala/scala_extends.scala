class scala_extends {

}
object scala_extends{
  def main(args: Array[String]): Unit = {

  }
}
class persion{
  var name=""
  override def toString=getClass().getName+"[name="+name+"]"
}
class student extends persion {
  var number=0
  override def toString=super.toString+"[number="+number+"]"
}


