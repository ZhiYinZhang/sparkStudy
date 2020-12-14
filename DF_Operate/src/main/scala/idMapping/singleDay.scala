package idMapping

import org.apache.commons.lang.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions.udf
import scala.collection.mutable.ListBuffer

/**
 * 利用spark graphx实现id mapping
 * 这个是单天的数据做id mapping，没有做多天滚动更新
 */
object singleDay {
	def main (args: Array[String]): Unit = {
		val spark=SparkSession.builder()
				.appName("idMapping")
				.master("local[*]")
				.getOrCreate()
		import spark.implicits._
		spark.sparkContext.setLogLevel("warn")

		val df=Seq(
			("13866778899","刘德华","wx_hz",2000),
			("13877669988","华仔","wx_hz",3000),
			("","刘德华","wx_ldh",5000),
			("13912344321","马德华","wx_mdh",12000),
			("13912344321","二师兄","wx_bj",3500),
			("13912664321","猪八戒","wx_bj",5600)
		).toDF("phone","name","wechat","value")
				.drop("value")


		//----------第一步构造点RDD
		//spark的图计算api中，点的集合是一个rdd里面包含点，而点是表示成一个tuple -> (点的唯一标识:Long,点的数据:String)
		//构造一个点rdd，这里前三个字段作为标识，需要转成点
		val vertices: RDD[(Long, String)] = df.rdd.flatMap(row => {
			val buffer: ListBuffer[Tuple2[Long, String]] = new ListBuffer[Tuple2[Long, String]]()
			//将一行数据中所有标识转成点，放到集合中
			for (i <- 0 until row.size) {
				val dotData = row.getString(i)
                //过滤掉""和null的数据；因为其他用户包含""，两个用户就会连在一起，因为数据hashcode相等
				//isNotBlank包括："",null," ",isNotEnpty包括:"",null
				if(StringUtils.isNotBlank(dotData)){
					 //点的唯一标识可以取点数据的hashcode，然后转Long
					 val dot = (dotData.hashCode.toLong, dotData)
					 buffer += dot
				 }
			}
			buffer
		})
//    vertices.foreach(println)
		/**
		 *vertices中的数据
    (208397334,13866778899)
(20977295,刘德华)
(113568560,wx_hz)
(-1485777898,13877669988)
(681286,华仔)
(113568560,wx_hz)
(20977295,刘德华)
(-774338670,wx_ldh)
(-1095633001,13912344321)
(38771171,马德华)
(-774337709,wx_mdh)
(-1095633001,13912344321)
(20090824,二师兄)
(113568358,wx_bj)
(-1007898506,13912664321)
(29003441,猪八戒)
(113568358,wx_bj)
		 */

		//----------第二步构造边RDD
		//spark graphx中对边的描述结构：Edge(起始点id:Long,目标点id:Long,边数据:String)
		val edges: RDD[Edge[String]] = df.rdd.flatMap(row => {
			var row_list=for(i <- 0 until row.size) yield row.getString(i)
			//过滤掉""," ",null的点数据
			row_list=row_list.filter(StringUtils.isNotBlank(_))
			//Edge的类型为边数据的类型
			val buffer = ListBuffer[Edge[String]]()
			for (i <- 0 until row_list.size - 1) { //为了取目标点时不超出范围,长度要减一;这要这些点可以连起来就行，不需要构成一个有环图
				//目标点为下一个点；边数据其实是一个属性值，可以表示这个边的含义，但是我们这里不用
				val edge = Edge(row_list(i).hashCode.toLong, row_list(i + 1).hashCode.toLong, "")
				buffer += edge
			}
			buffer
		})

		edges.foreach(println)


		/**
		Edge(-1095633001,38771171,)
    Edge(-1485777898,681286,)
    Edge(-1095633001,20090824,)
    Edge(208397334,20977295,)
    Edge(20090824,113568358,)
    Edge(681286,113568560,)
    Edge(-1007898506,29003441,)
    Edge(38771171,-774337709,)
    Edge(29003441,113568358,)
    Edge(0,20977295,)
    Edge(20977295,-774338670,)
    Edge(20977295,113568560,)
		 */


		//----------第三步构造图
		val graph: Graph[String, String] = Graph(vertices, edges)

		//调用图的算法:连通子图算法
		//得到还是一个图;VertexId就是Long类型
		val graph2: Graph[VertexId, String] = graph.connectedComponents()

		//从结果图中，取出图的点集合，即可以得到我们想要的分组结果
		val vertices1: VertexRDD[VertexId] = graph2.vertices
		vertices1.foreach(println)

		/**
		 * (点id,点数据)，点数据相同的点表示属于同一组的
    (29003441,-1095633001)
    (-1485777898,-1485777898)
    (-1007898506,-1095633001)
    (-774338670,-1485777898)
    (-1095633001,-1095633001)
    (113568560,-1485777898)
    (0,-1485777898)
    (-774337709,-1095633001)
    (20090824,-1095633001)
    (20977295,-1485777898)
    (38771171,-1095633001)
    (113568358,-1095633001)
    (681286,-1485777898)
    (208397334,-1485777898)
		 */
		//    graph2.edges


		//----------使用
		/**
		 * 目标：在我们原始数据中得到新一列，这一列就是点数据，即用户唯一标识
		 * 做法一：
		 * 可以将原始数据任一标识字段的数据转成hashcode，然后与映射关系rdd进行join，以点id为join key(因为点id也是从标识字段hashcode转过来的)
		 * 做法二：
		 * 将映射关系rdd转成map，然后广播出去，原始数据去匹配
		 *
		 *需要注意的是，原始数据第三行的第一个数据为"",如果另一个用户也有一个数据为"",那么构造图的时候，就会将两个用户构成一个连通子图，
		 *这种数据应该算脏数据，所以上面在构造点RDD和边RDD的时候，应该过滤掉
		 */


		//-----做法一
		//不能使用functions下面的hash函数，因为上面我们是使用scala的hash函数，对于同一数据两个得到的值是不一样的
		val hashcode_udf=udf((x:String)=>x.hashCode.toLong,LongType)
		val df1=df.withColumn("dotId",hashcode_udf($"phone"))

		//结果图转成dataframe
		val vertricesDF = vertices1.toDF("dotId", "dotData")

		df1.join(vertricesDF,"dotId").show()

		/**
		+---------------+-----------+------+------+-----------+
    |      dotId       |      phone      |  name|wechat  |    dotData|
    +-----------+-----------------+--------+----------+--------------+
    |-1095633001|13912344321|二师兄|    wx_bj|-1095633001|
    |-1095633001|13912344321|马德华|   wx_mdh|-1095633001|
    |          0|           |刘德华|   wx_ldh|-1485777898|
    |  208397334|13866778899|刘德华|    wx_hz|-1485777898|
    |-1485777898|13877669988| 华仔|     wx_hz|-1485777898|
    |-1007898506|13912664321|猪八戒|    wx_bj|-1095633001|
    +-----------+-----------+------+------+-----------+
		 */


		//-----做法二
		//将映射关系rdd收集到Driver端
		val idmpMap: collection.Map[VertexId, VertexId] = vertices1.collectAsMap()
		//广播
		val bc = spark.sparkContext.broadcast(idmpMap)

		df.map(row=>{
			val bc_idmapMap = bc.value
			//任一不是空的标识数据
			var row_list=for(i <- 0 until row.size) yield row.getString(i)
			//过滤掉""," ",null的点数据
			row_list=row_list.filter(StringUtils.isNotBlank(_))
			val key=row_list(0).hashCode.toLong

			val gid: VertexId = bc_idmapMap(key)

			(row.getString(0),row.getString(1),row.getString(2),gid)
		}).toDF("phone","name","wechat","dotData").show()

		//或者在udf里面使用广播变量
//    val get_idmp=udf((x:String)=>bc.value(x.hashCode.toLong),LongType)
//    df.withColumn("dotData",get_idmp($"phone")).show()

		/**
		+-----------+------+------+-----------+
    |      phone|  name|wechat|    dotData|
    +-----------+------+------+-----------+
    |13866778899|刘德华| wx_hz|-1485777898|
    |13877669988|  华仔| wx_hz|-1485777898|
    |           |刘德华|wx_ldh|-1485777898|
    |13912344321|马德华|wx_mdh|-1095633001|
    |13912344321|二师兄| wx_bj|-1095633001|
    |13912664321|猪八戒| wx_bj|-1095633001|
    +-----------+------+------+-----------+
		 */



		spark.close()
	}
}
