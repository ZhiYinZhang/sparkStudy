import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col


object spark_write_hbae_non_kerberos {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = HBaseConfiguration.create()

    val spark = SparkSession.builder().appName("read_hbase")
                            .master("local[3]")
                            .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("WARN")


    val df=spark.read.option("header",true).csv("E:\\test\\retail")
      .where((col("STATUS")=!="04"))
      .na.drop("any",Seq("CITY","CUST_NAME"))
      .select("CUST_ID","CUST_NAME","GRADE","STATUS","CITY","SALE_CENTER_ID")

    df.printSchema()

    spark_write_hbase(df,conf,Map("table"->"TEST","cf"->"0","row_key"->"CUST_ID"),List("CUST_NAME","STATUS","CITY","SALE_CENTER_ID"))

  }

  def spark_write_hbase(df:DataFrame,conf:Configuration,param:Map[String,String],columns:List[String]): Unit ={
    /**
      * param "table"<-"","cf"<-"","row_key"<-""
      */

    val table=param("table")
    val cf=param("cf")
    val row_key=param("row_key")

    val job: Job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    val jobConf=job.getConfiguration
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,param("table"))

    df.limit(1000).rdd.map(x=>{
      val put = new Put(Bytes.toBytes(x.getAs[String](row_key)))

      for(column <- columns){
        val value=x.getAs[String](column)
        //        println(column,value)
        //        try{
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value))
        //        }catch{
        //          case ex:Exception=>println("exception:",column,value)
        //        }

      }
      (new ImmutableBytesWritable,put)
    }).saveAsNewAPIHadoopDataset(jobConf)
  }
}
