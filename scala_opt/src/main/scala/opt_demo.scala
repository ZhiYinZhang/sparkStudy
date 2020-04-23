


//import java.sql.Timestamp
import java.sql.Connection
import java.text.SimpleDateFormat

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.joda.time.{DateTime, Days}
import org.joda.time.Duration
import org.joda.time.ReadableDateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.collection.mutable.{ListBuffer, Map}

object opt_demo {
  def main(args: Array[String]): Unit = {
        val mills_per_second=1000L
        val seconds_per_day=24*60*60L
        val mills_per_day=seconds_per_day*mills_per_second




        val params=Map("driverClass"->"com.mysql.jdbc.Driver",
          "jdbcUrl"->"jdbc:mysql://localhost:3306/test?useSSL=false&characterEncoding=utf-8",
          "user"->"root","password"->"123456")
        val conn=getDBConnect(params)

        val tableName="test"
        val startOffset="2019-12-23 11:00:00"
        val endOffset="2019-12-23 14:00:00"
    //        select * from test where (createTime>'2019-12-23 11:00:00' and createTime<='2019-12-23 13:00:00') or (updateTime>'2019-12-23 11:00:00' and updateTime<='2019-12-23 13:00:00')
        val sql=s"select * from ${tableName} where (createTime>'${startOffset}' and createTime<='${endOffset}') or (updateTime>'${startOffset}' and updateTime<='${endOffset}') "
        println(sql)
        val st = conn.prepareStatement(sql)
        val rs = st.executeQuery()
        while (rs.next()) {
          val createTime = rs.getTimestamp("createTime")
          val updateTime = rs.getTimestamp("updateTime")
          val days: java.lang.Long = updateTime match {
            case null => null
            case value => value.getTime/mills_per_day
          }
          val updateTimeDay=new DateTime(days*mills_per_day)
          println(createTime, updateTime, updateTimeDay)


        }

  }

  def getDBConnect(paramters:Map[String,String]):Connection={
    val driverClass = paramters("driverClass")
    val jdbcUrl = paramters("jdbcUrl")
    val user=paramters("user")
    val password=paramters("password")

    val source: ComboPooledDataSource = new ComboPooledDataSource()
    source.setDriverClass(driverClass)
    source.setJdbcUrl(jdbcUrl)
    source.setUser(user)
    source.setPassword(password)
    source.setInitialPoolSize(10)
    source.setMaxPoolSize(30)
    source.setMinPoolSize(10)
    source.setAcquireIncrement(5)

    val connection: Connection = source.getConnection()
    connection
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