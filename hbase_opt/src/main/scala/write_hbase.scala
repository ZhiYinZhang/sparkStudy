import java.sql.Time
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation
import scala.util.Random


object write_hbase {
  def main(args: Array[String]): Unit = {
    val krb5_conf=this.getClass.getResource("krb5.conf").getPath
    val user_keytab=this.getClass.getResource("zhangzy.keytab").getPath
    System.setProperty("java.security.krb5.conf",krb5_conf)

    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hadoop.security.authentication", "Kerberos")
    //    conf.set(TableOutputFormat.OUTPUT_TABLE,"test_ma")

    UserGroupInformation.setConfiguration(conf)
    val ugi=UserGroupInformation.loginUserFromKeytabAndReturnUGI("zhangzy@HADOOP.COM",user_keytab)
    UserGroupInformation.setLoginUser(ugi)

    val table = new HTable(conf,"test_ljs")

    println(new Date())
    var put:Put = null
    val arr=new java.util.ArrayList[Put]
    for(i <- 1 to 1000){
      put=new Put(Bytes.toBytes(i.toString))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("number"),Bytes.toBytes(Random.nextInt(100).toString))
      arr.add(put)
    }
    table.put(arr)

    println(new Date())
    table.close()

  }
}
