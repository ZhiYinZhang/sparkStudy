import java.sql.{Connection, PreparedStatement}

import com.mchange.v2.c3p0.ComboPooledDataSource

object mysql_opt {
  def main(args: Array[String]): Unit = {
    val driverClass = "com.mysql.jdbc.Driver"
    val jdbcUrl = "jdbc:mysql://localhost:3306/entrobus"
    val user = "root"
    val password = "123456"

    val dataSource = new ComboPooledDataSource()
    dataSource.setDriverClass(driverClass)
    dataSource.setJdbcUrl(jdbcUrl)
    dataSource.setUser(user)
    dataSource.setPassword(password)
    dataSource.setInitialPoolSize(40)
    dataSource.setMaxPoolSize(100)
    dataSource.setMinPoolSize(10)
    dataSource.setAcquireIncrement(5)

    val conn: Connection = dataSource.getConnection()

    for(i <- 0 to 100){
      Thread.sleep(100)
      val sql = s"insert into product values(null,$i,3,4)"
      val st: PreparedStatement = conn.prepareStatement(sql)
      st.execute(sql)
    }

//    val sql = "select * from product limit 10 offset 0"
//    val st: PreparedStatement = conn.prepareStatement(sql)
//    val rs: ResultSet = st.executeQuery()
//
//    while(rs.next()){
////      println(rs.getString("pid"),rs.getString("pname"),rs.getString("price"),rs.getString("category_id"))
////      println(rs.getLong(1))
//
//    }

    conn.close()


  }
}

