import java.sql.DriverManager

object presto_opt {
def main(args:Array[String]):Unit={
  val url = "jdbc:presto://node3.hadoop.com:8081/hive/log"
  val connection = DriverManager.getConnection(url,"root",null)

  val stat = connection.createStatement()
  val rs=stat.executeQuery("show tables")

  while(rs.next()){
    
    println(rs.getString(1))
  }

  rs.close()
  connection.close()


  }
}
