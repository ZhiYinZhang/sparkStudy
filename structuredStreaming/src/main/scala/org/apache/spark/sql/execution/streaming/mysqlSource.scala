package org.apache.spark.sql.execution.streaming

import java.sql.{Connection, ResultSet}

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
/*
* 自定义数据输入源：需要继承Source接口   在项目下创建包名：org.apache.spark.sql.execution.streaming
  *       实现思路：
  *       （1）通过重写schema方法来指定数据输入源的schema，这个schema需要与Provider中指定的schema保持一致
  *       （2）通过重写getOffset方法来获取数据的偏移量，这个方法会一直被轮询调用，不断的获取偏移量
  *       （3) 通过重写getBatch方法，来获取数据，这个方法是在偏移量发生改变后被触发
  *       （4）通过stop方法，来进行一下关闭资源的操作
  *       其他的方法为用户自定义的
* */
class mysqlSource(sqlContext:SQLContext,
                  parameters:Map[String,String],
                  schemaOption:Option[StructType])
                 extends Source with Logging {

  lazy val conn: Connection = getDBSource(parameters)

  val tableName: String = parameters("tableName")

  //初始化偏移量
  var currentOffset: Map[String, Long] = Map[String, Long](tableName -> 0)
  //每个batch最大的偏移量

  val maxOffsetPerBatch :Option[Long]= Option(parameters.getOrElse("maxOffsetPerBatch","100").toLong)

  val inputMetrics = new InputMetrics()

  /*
  * 指定数据源的schema，需要与Provider中sourceSchema中指定的schema保持一致，否则会报异常
  * 触发机制：当创建数据源的时候被触发执行
  *
  * @return schema
  * */
  override def schema: StructType = schemaOption.get

  /*
  * 获取offset，用来监控数据的变化情况
  * 触发机制：不断轮询调用
  * 实现要点：
  * 一、offset的实现
  * 用函数返回值可以看出，我们需要提供一个标准的返回值Option[Offset]
  * 我们可以通过继承org.apache.spark.sql.sources.v2.reader.streaming.Offset实现，这里面其实就是保存一个json字符串
  * 二、json转换
  * 因为offset里实现的是一个json字符串，所以我们需要将我们存放offset的集合或者case class转换成json字符串
  * spark里是通过org.json4s.jackson这个包实现case class集合类(Map,List,Seq,Set等)与json字符串的相互转换
  *
  * @return offset
  * */
  override def getOffset: Option[Offset] = {
    val latest = getLatestOffset
    val offsets = maxOffsetPerBatch match {
      case None => map2Offset(latest)
      case Some(limit) => //limit = maxOffsetPerBatch
        map2Offset(rateLimit(limit, currentOffset, latest))
    }
    Option(offsets)

  }

  /*
  * 获取数据
  *
  * @param start 上一个批次的end offset
  * @param end 通过getOffset获取的新的offset
  * 触发机制：当不断轮询的getOffset方法，获取的offset发生改变时，会触发该方法
  * 实现要点：
  * 一、DataFrame的创建：
  *     可以通过生成RDD，然后使用RDD创建DataFrame
  *     RDD创建：sqlContext.sparkContext.parallelize(rows.toSeq)
  *     DataFrame创建：sqlContext.internalCreateDataFrame(rdd,schema,isStreaming=true)
  *
  * @return DataFrame
  * */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    var offset: Long = 0
    if (start.isDefined) {
      offset = offset2Map(start.get)(tableName)
    }

    val limit = offset2Map(end)(tableName) - offset

    logInfo(s"limit:$limit,startOffset:$offset")
    val sql = s"SELECT * FROM $tableName limit $limit offset $offset"

    val st = conn.prepareStatement(sql)
    val rs = st.executeQuery()

    //spark里面的jdbcUtils工具类，将查询结果转成InternalRow
    //但是这里有问题，一个batch里面的数据都会变成和最后一条数据一样
//    val rows: Iterator[InternalRow] = JdbcUtils.resultSetToSparkInternalRows(rs, schemaOption.get, inputMetrics)

    val rows = getInternalRow1(rs)


    val rdd = sqlContext.sparkContext.parallelize(rows.toSeq)

    currentOffset = offset2Map(end)

    sqlContext.internalCreateDataFrame(rdd, schema,  true)
  }

  /*
  * 关闭资源
  * 将一些需要关闭的资源放到这里来关闭，如mysql的数据库的连接等
  * */
  override def stop(): Unit = {
    conn.close()
  }

  /*-----------------------------------------------------*/
  /*
  * 获取本次batch的数据，转成InternalRow  专用
  * */
  def getInternalRow0(rs:ResultSet):Iterator[InternalRow]={
    var rows:List[InternalRow]=List[InternalRow]()
    while(rs.next()){
      //注意类型  基本数值类型要转成包装器类型     string转成utf8
      val internalRow: InternalRow = InternalRow(new Integer(rs.getInt("pid"))
                                                , UTF8String.fromString(rs.getString("pname"))
                                                ,new Integer(rs.getInt("price"))
                                                ,UTF8String.fromString(rs.getString("category_id"))
                                     )
      rows = rows.:+(internalRow)
    }
     rows.toIterator
  }
  /*
  通用版   有待增强  可以参考 JdbcUtils.resultSetToSparkInternalRows#makeGetter
  * 获取本次batch的数据，转成InternalRow
  * 在re.next()中：
  * 1.遍历字段，根据schema中每个字段的类型，rs使用相应的api获取对应类型的字段数据
  * 2.将数据追加到Seq中
  * 3.将使用InternalRow.fromSeq()将Seq转成InternalRow
  * 4.将InternalRow追加到List中
  *
  * */
  def getInternalRow1(rs:ResultSet):Iterator[InternalRow]={
    //获取StructType
    val schema: StructType = schemaOption.get
    val fields: Array[StructField] = schema.fields

    var rows:List[InternalRow]=List[InternalRow]()
    while(rs.next()){
        var s = Seq[Any]()
        //遍历字段 根据字段类型 去获取对应的数据
        //注意类型  基本数值类型要转成包装器类型     string转成utf8
        for(field<-fields){
    //        println(field.name,field.dataType)
            val value = field.dataType match{
              case IntegerType => rs.getInt(field.name)
              case StringType => UTF8String.fromString(rs.getString(field.name))
              case LongType =>rs.getLong(field.name)
              case TimestampType =>{
                val t:java.lang.Long=rs.getTimestamp(field.name) match{
                  case null=>null
                    //时间戳在IntervalRow里面必须是微秒单位的
                  case value=>value.getTime*1000
                  }
                t
              }
              case DoubleType => rs.getDouble(field.name)
            }
            s=s.:+(value)
        }
  //      println(s)
        //将Seq转成InternalRow
        val internalRow = InternalRow.fromSeq(s)
        rows = rows.:+(internalRow)
    }
    rows.toIterator
  }

  /*
  * 判断当前batch数据量(latestOffset-currentOffset)是否超过 maxOffsetPerBatch
  *        超过就只返回 maxOffsetPerBatch条数据
  *        没超过就返回 (latestOffset-currentOffset)条数据
  * */
  def rateLimit(limit: Long, currentOffset: Map[String, Long], latestOffset: Map[String, Long]): Map[String, Long] = {
    val co = currentOffset(tableName)
    val lo = latestOffset(tableName)
    if (co + limit > lo) {
      Map[String, Long](tableName -> lo)
    } else {
      Map[String, Long](tableName -> (co + limit))
    }
  }

  // 获取数据源最新偏移量
  def getLatestOffset: Map[String, Long] = {
    var offset: Long = 0
    val sql = s"SELECT COUNT(1) FROM $tableName"
    val st = conn.prepareStatement(sql)
    val rs: ResultSet = st.executeQuery()
    while (rs.next()) {
      offset = rs.getLong(1)
    }
    Map[String, Long](tableName -> offset)
  }


 //获取数据库连接
  def getDBSource(paramters:Map[String,String]):Connection={
    val driverClass = paramters("driverClass")
    val jdbcUrl = paramters("jdbcUrl")
    val user = paramters("user")
    val password = paramters("password")

    val dataSource = new ComboPooledDataSource()
    dataSource.setDriverClass(driverClass)
    dataSource.setJdbcUrl(jdbcUrl)
    dataSource.setUser(user)
    dataSource.setPassword(password)
    dataSource.setInitialPoolSize(40)
    dataSource.setMaxPoolSize(100)
    dataSource.setMinPoolSize(10)
    dataSource.setAcquireIncrement(5)

    dataSource.getConnection
  }
  //将 Offset 转换成 map
  def offset2Map(offset: Offset): Map[String, Long] = {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    Serialization.read[Map[String, Long]](offset.json())
  }
}
//将map 转换成 Offset
case class map2Offset(offset: Map[String, Long]) extends Offset {
  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  override def json(): String = Serialization.write(offset)
}
