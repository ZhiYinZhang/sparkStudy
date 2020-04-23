package org.apache.spark.sql.execution.streaming


import java.sql.{Connection, ResultSet, Timestamp}

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DateType, Decimal, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType, VarcharType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import scala.collection.mutable.ListBuffer

class customMysqlSource(sqlContext:SQLContext,
                        paramters:Map[String,String],
                        schemaOption:Option[StructType])
  extends Source with Logging {
  //获取数据库连接
  val connect=getDBConnect(paramters)

  val tableName=paramters("tableName")

  //获取日期解析 pattern default: yyyy-MM-dd HH:mm:ss
  val format=DateTimeFormat.forPattern(paramters.getOrElse("pattern","yyyy-MM-dd HH:mm:ss"))

  //当前偏移量
  val currentTime=paramters.get("currentTime") match{
    case None=>DateTime.now().getMillis
    case value:Option[String]=>format.parseDateTime(value.get).getMillis
  }
  val currentOffset:Map[String,Long]= Map(tableName->currentTime)



  override def schema: StructType = schemaOption.get
  //在getBatch里面，将sql查询结果转换成InternalRow时使用
  val inputMetrics=new InputMetrics()
  /**
    * 获取offset
    * @return
    */
  override def getOffset: Option[Offset] = {
     val latestOffset=getLatestOffset

     Some(map2Offset(latestOffset))
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {

    //第一次start是没有定义的，将start初始值为currentTime
    var startOffset:String=time2String(currentOffset(tableName))
    if(start.isDefined){
         startOffset=time2String(offset2Map(start.get)(tableName))
    }
    val endOffset=time2String(offset2Map(end)(tableName))


    val sql=s"select * from ${tableName} where (createTime>'${startOffset}' and createTime<='${endOffset}') or (updateTime>'${startOffset}' and updateTime<='${endOffset}') "
    println(sql)
    val st=connect.prepareStatement(sql)
    val rs=st.executeQuery()

    /**
      * 1.将查询结果放到集合里面
      * 这个缺点需要确定每个字段的类型
      * 可以转成json字符串  {"a":1,"b":2},然后在使用时在转换
      */
    //使用scala的Long，如果值为null，在ListBuffer里面会报空指针异常
//    val data=new ListBuffer[(Int,java.lang.Long,java.lang.Long)]
//    while(rs.next()){
//      //时间戳类型的数据在IntervalRow是微秒，Timestamp.getTime是毫秒
//      val f1=rs.getInt(1)
//      val f2:java.lang.Long=rs.getTimestamp(2).getTime*1000
//      val f3:java.lang.Long=rs.getTimestamp(3) match{
//        case null=>null
//        case value=>value.getTime*1000
//      }
//      data.append((f1,f2,f3))
//    }
//    val rows: ListBuffer[InternalRow] = data.map(x=>InternalRow(x._1,x._2,x._3))
//    val value: RDD[InternalRow] = sqlContext.sparkContext.parallelize(rows)
//    sqlContext.internalCreateDataFrame(value,schema,true)


    /**
      *2.使用spark内置的jdbc工具类，将结果转换成InternalRow
      * 问题： seq里面的数据是正常的，使用sqlContext.sparkContext.parallelize(seq)之后
      * 数据都变成和最后行一样
      */

    val rows: Iterator[InternalRow] = JdbcUtils.resultSetToSparkInternalRows(rs,schema,inputMetrics)

    val seq: List[InternalRow] = rows.toList

    seq.foreach(x=>{
      val create=new DateTime((x.getLong(1)/1000).toLong)
      val update=new DateTime((x.getLong(2)/1000).toLong)
      println(x.getInt(0),create,update)
    })

    val sc=sqlContext.sparkContext
    val value: RDD[InternalRow] = sc.parallelize(rows.toSeq)
    value.collect().foreach(println(_))

    currentOffset.updated(tableName,offset2Map(end))
    sqlContext.internalCreateDataFrame(value,schema).show()

    sqlContext.internalCreateDataFrame(value,schema,true)
  }





//  override def commit(end: Offset): Unit = {
//    val endOffset=offset2Map(end)(tableName)
//    currentOffset.updated(tableName,endOffset)
//  }

  override def stop(): Unit ={
    connect.close()
  }

  /**************************************/
  def convertIntervalRow(rs:ResultSet):Iterator[InternalRow]={
    val fields: Array[StructField] = schema.fields
    var rows: List[InternalRow] = List[InternalRow]()
    while(rs.next()){
      var s = Seq[Any]()
      //根据字段的类型去获取数据
      for(field<-fields){
        val colName=field.name
        val value=field.dataType match{
          case ShortType=>rs.getShort(colName)
          case IntegerType=>rs.getInt(colName)
          case LongType=>rs.getLong(colName)
          case FloatType=>rs.getFloat(colName)
          case DoubleType=>rs.getDouble(colName)
          case StringType=>UTF8String.fromString(rs.getString(colName))
          case BooleanType=>rs.getBoolean(colName)
          case DateType=>rs.getDate(colName)//要转成天数
          case TimestampType =>{
            val t:java.lang.Long=rs.getTimestamp(field.name) match{
              case null=>null
              //时间戳在IntervalRow里面必须是微秒单位的
              case value=>value.getTime*1000
            }
            t
          }//要转成微秒
        }
        s=s.:+(field)
      }
      val internalRow = InternalRow.fromSeq(s)
      rows=rows.:+(internalRow)
    }

    rows.toIterator
  }
  def getLatestOffset:Map[String,Long]={
    //直接使用当前时间
//    val latestOffset=DateTime.now().getMillis

    //获取currentTime到现在最大的时间戳
    val sql=s"select max(createTime),max(updateTime) from ${tableName} where createTime>'${currentTime}' or updateTime>'${currentTime}' "
    val st=connect.prepareStatement(sql)
    val rs=st.executeQuery()

    var latestOffset:Long=0L
    while(rs.next()){
      val maxCreateTime=rs.getTimestamp(1).getTime
      val maxUpdateTime=rs.getTimestamp(2).getTime
      latestOffset=Seq(maxCreateTime,maxUpdateTime).max
    }
      Map(tableName->latestOffset)
  }
  /**
    * 数据库连接池
    * @param paramters
    * @return
    */
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
  def time2String(time:Long):String={
    //将毫秒值转成string
    val dt=new DateTime(time)
    dt.toString(format)
  }
  def offset2Map(offset:Offset):Map[String,Long]={
     implicit val formats:AnyRef with Formats=Serialization.formats(NoTypeHints)
     Serialization.read[Map[String,Long]](offset.json())
}
  case class map2Offset(offset:Map[String,Long]) extends Offset{
    implicit val formats:AnyRef with Formats=Serialization.formats(NoTypeHints)
    override def json():String=Serialization.write(offset)
  }
}
