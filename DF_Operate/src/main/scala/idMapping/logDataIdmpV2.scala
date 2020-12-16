package idMapping

import org.apache.commons.lang.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, col}
/**
 * 三类埋点日志的id映射计算程序
 * 考虑滚动整合
 */
object logDataIdmpV2 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("warn")
    import spark.implicits._

    val path="e://data//logData//"
    val appData=spark.read.option("header",true).csv(path+"app.csv")
    val webData=spark.read.option("header",true).csv(path+"web.csv")
    val wechatData=spark.read.option("header",true).csv(path+"wechat.csv")

    val cols=appData.columns.map(col(_))
    //将所有标识列放到数组中,并拼接所有日志数据
    val allTag=appData.select(array(cols:_*).alias("allTag"))
      .union(webData.select(array(cols:_*).alias("allTag")))
      .union(wechatData.select(array(cols:_*).alias("allTag")))

    //1.构造点rdd
    val vertices: RDD[(Long, String)] = allTag.rdd.flatMap(row => {
      //过滤掉空的数据
      val filter = row.getSeq[String](0).filter(StringUtils.isNotBlank(_))
      filter.map(tag => (tag.hashCode.toLong, tag))
    })

    //2.构造边rdd
    /**
     *过滤弱关联边：即过滤掉出现次数比较少的边，如其他的人使用我的手机登陆了账号，那么应该过滤掉他,这个阈值就是经验了
     *在构造边的时候应该两两连接，然后再去统计每个边的数量，过滤掉数量少的
     */
    val edges=allTag.rdd.flatMap(row=>{
      //过滤掉空的数据
      val filter=row.getSeq[String](0).filter(StringUtils.isNotBlank(_))
      //使用双层for循环
      for(i<-0 until filter.size-1;j<-i+1 until filter.size) yield Edge(filter(i).hashCode.toLong,filter(j).hashCode.toLong,"")
    })
    val filter_edges=edges.map(edge=>(edge,1))
      .reduceByKey(_+_)//计数
      .filter(_._2>2)//过滤掉次数小于等于2的边
      .map(tp=>tp._1)


    //3.将上一日的idmapping映射字典，解析成点，边集合
    val lastDayIdmp=spark.read.parquet("e://data//idMapping_res//20201215")
    //构造上一日的点集合
    val lastDayVertices=lastDayIdmp.rdd.map(r=>{
      //第一个就是点，数据为空
      (r.getLong(0),"")
    })
    //构造上一日的边集合
    val lastDayEdges=lastDayIdmp.rdd.map(r=>{

      Edge(r.getAs[VertexId](0),r.getAs[VertexId](1),"")
    })




    //3.构造图
    val graph=Graph(vertices,filter_edges)
    val graph2=graph.connectedComponents()


    val vertices1=graph2.vertices

    //保存结果
    vertices1.toDF("tag_hashcode","guid")
      .write
      .parquet("e:://data//idMapping_res//20201215")

  }
}
