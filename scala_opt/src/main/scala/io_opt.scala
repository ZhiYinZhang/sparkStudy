
import java.io._
import java.util.Scanner

import scala.io._
object io_opt {
  def main(args: Array[String]): Unit = {
    val path0="E:\\test\\rate\\batchId.txt"
    val path1="e://test//checkpoint//URL1.txt"


     new LineNumberReader(new FileReader(path1))

     new PushbackReader(new StringReader(path1))

  }

  def readFile0(path:String)={
    val br = new BufferedReader(new FileReader(path))
    //数组的长度是每次读取的长度
    // 换行符占两个，不能截断，如一行 10个字符，取11的话读不出来
    val chars: Array[Char] =new Array[Char](65)
//    br.readLine()
    br.read(chars)
    chars.foreach(print)
    br.close()
  }
  def readFile1(path:String)={
    /**
      *以字节流输入  通过InputStreamReader转成字符流
      * 可以指定编码
      */
    val fis = new FileInputStream(path)
    val br = new BufferedReader(new InputStreamReader(fis,"utf-8"))
    println(br.readLine())

//    var record:String=null
//    while((record=br.readLine())!=null){
//      println(record)
//    }

    br.close()
  }
}
