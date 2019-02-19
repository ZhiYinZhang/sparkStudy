package org.apache.spark.sql.execution.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType


/*
*自定义StructuredStreaming数据源     在项目下创建包名:org.apache.spark.sql.execution.streaming
* 1.继承DataSourceRegister类
*    需要重写shortName方法，用来向spark注册该组件
* 2.继承StreamSourceProvider类
*    需要重写createSource以及sourceSchema方法，用来创建数据输入源
* 3.继承StreamSinkProvider类
*    需要重写createSink方法，用来创建数据输出源
* */
class mysqlSourceProvider extends DataSourceRegister
                          with StreamSourceProvider
                          with Logging{
  /*
     * 数据源的name，如kafka，csv
     * @return 字符串shotName
     * */
  override def shortName(): String = "mysql"

  /*
  * 定义数据源的schema
  *
  * @param sqlContext Spark sql上下文
  * @param schema 通过 .schema()方法传入的schema
  * @param providerName Provider的名称，包名+类名
  * @param parameters 通过 .option()方法传入的参数
  *
  * @return 元组，(shotName,schema)
  * */
  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String, parameters: Map[String, String]): (String, StructType) = (shortName(),schema.get)

  /*
  * 创建数据源
  *
  * @param sqlContext Spark sql上下文
  * @param metadataPath 元数据path
  * @param schema 通过 .schema()方法掺入的schema
  * @param providerName Provider的名称，包名+类名
  * @param parameters 通过 .option()方法传入的参数
  *
  * @return 自定义source，需要继承Source接口实现
  * */
  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source =new mysqlSource(sqlContext,parameters,schema)

}
