import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.{HTable, Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation

object read_hbase {
  def main(args: Array[String]): Unit = {

    val krb5_conf = "e://dataset//krb5.conf"
    val user_krbtab = "e://dataset//zhangzy.keytab"

    System.setProperty("java.security.krb5.conf", krb5_conf)
    val conf: Configuration = HBaseConfiguration.create()
    //    conf.set("hbase.zookeeper.quorum", "10.18.0.12") // zookeeper地址
    //    conf.set("hbase.zookeeper.property.clientPort", "2181") // zookeeper端口
    conf.set("hadoop.security.authentication", "Kerberos")
    //    conf.set("hbase.security.authentication", "Kerberos")
    //    conf.set("zookeeper.znode.parent", "/hbase")
    //    //        conf.set("hbase.regionserver.kerberos.principal", "hbase/entrobus12@HADOOP.COM");
    //    conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@HADOOP.COM")
    //    conf.set("hbase.master.kerberos.principal", "hbase/entrobus12@HADOOP.COM")

    UserGroupInformation.setConfiguration(conf)
    try {
      val gui = UserGroupInformation.loginUserFromKeytabAndReturnUGI("zhangzy@HADOOP.COM", user_krbtab)
      UserGroupInformation.setLoginUser(gui)
    } catch {
      case e: Exception => {
        println(e.getMessage)
      }
    }

    val table = new HTable(conf, "testtable")
    val scan = new Scan()
    val scanner: ResultScanner = table.getScanner(scan)

    var result: Result = scanner.next()
    println("result:",Bytes.toString(result.value()))
    while(!result.isEmpty){
      //      val values: Array[KeyValue] = result.raw()
      val cells: Array[Cell] = result.rawCells()
      for (cell <- cells) {
        val array: Array[Byte] = cell.getValueArray
        println(new String(cell.getRowArray, "UTF-8"))

      }
      result=scanner.next()
    }

  }
}
