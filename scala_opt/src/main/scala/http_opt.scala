import scalaj.http._
object http_opt {
  def main(args: Array[String]): Unit = {
    val url = "http://tool.chinaz.com/Tools/urlencode.aspx"
    val jpg="https://ss0.bdstatic.com/70cFvHSh_Q1YnxGkpoWK1HF6hhy/it/u=219923786,1011791638&fm=27&gp=0.jpg"
    val flume="http://10.18.0.28:10002/someuri"
     val rp = Http(flume).asString
    println(rp.body)

  }
}
