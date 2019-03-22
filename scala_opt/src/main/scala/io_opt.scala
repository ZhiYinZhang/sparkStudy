
import java.io._
import java.util.Scanner

import scala.io._
object io_opt {
  def main(args: Array[String]): Unit = {
    val path0="E:\\test\\rate\\batchId.txt"
    val path1="e://test//checkpoint//URL1.txt"


    val base="e://dataset//question//question2//"
    val file = base+"words.txt"
    val hash_file = base+"hash"

    val br = new BufferedReader(new FileReader(file))
    var bw:BufferedWriter = null
    //定义缓冲区大小
    var records=new Array[Char](18*1000)
    var strs:Array[String]=null
    //每次读取的字符长度
    var len:Int=0
    while(len != -1){
      len=br.read(records)
      strs=new String(records.slice(0,len)).split("\n")
      var num=0
      for(i<-strs) {
              var hashId:Int=i.hashCode%5000
              hashId=hashId.abs
              bw=new BufferedWriter(new FileWriter(base+s"test//$hashId.txt",true))

              bw.write(i+"\n")
              bw.flush()
        println(s"$num/${len/18}" + "   " + i)
        num += 1
      }

      println("----------------------------------")
//      Thread.sleep(1000)
    }

    br.close()
    bw.close()
  }

  def readFile0(path:String)={
    val br = new BufferedReader(new FileReader(path))

    /**
      * 数组的长度是每次读取的长度
      *  1.如果最后读完，刚好将数组填满，没有问题，
      *  否则数组多余的为以null字符写入文件
      *  解决:num=br.read(chars)的返回值是本次读入的字符长度
      *       bw.write(chars,0,num)指定写入的长度
      * 换行符占两个，不能截断，如一行 10个字符，取11的话读不出来
      */

    val chars: Array[Char] =new Array[Char](65)
//    br.readLine()
    val num=br.read(chars)
    chars.slice(0,num).foreach(print)
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
  def readFile2()={
    val base="e://dataset//question//question2//"
    val file = base+"words.txt"
    val hash_file = base+"hash"

    val br = new BufferedReader(new FileReader(file))
    var bw:BufferedWriter = null
    //定义缓冲区大小
    var records=new Array[Char](18*1000)
    var strs:Array[String]=null
    //每次读取的字符长度
    var len:Int=0
    while(len != -1){
      //返回值为每次读取的字符长度
      len=br.read(records)
      strs=new String(records.slice(0,len)).split("\n")
      var num=0
      for(i<-strs) {
        var hashId:Int=i.hashCode%5000
        hashId=hashId.abs
        bw=new BufferedWriter(new FileWriter(base+s"test//$hashId.txt",true))

        bw.write(i+"\n")
        bw.flush()
        println(s"$num/${len/18}" + "   " + i)
        num += 1
      }
      println("----------------------------------")
    }
    br.close()
    bw.close()
  }
}
