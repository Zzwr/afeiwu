import java.sql.Connection
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object KSConsumer3 {
  def getConn(): Connection ={
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://hadoop102:3306/atguigu?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true")
    properties.setProperty("username", "root")
    properties.setProperty("password", "5859660Qq")
    properties.setProperty("maxActive","1000")
    val source: DataSource = DruidDataSourceFactory.createDataSource(properties)
    source.getConnection
  }
  def main(args: Array[String]): Unit = {

  }
}
