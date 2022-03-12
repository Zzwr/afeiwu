import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val test = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(test)
    val rdd1 = sc.makeRDD(Array((1,(1, 4)), (3,(2, 3)), (2,(3, 2))))
    val rdd3 = sc.makeRDD(Array((1,(1,3))))
    rdd1.foreach{
      case data=>{
        val properties = new Properties()
        properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
        properties.setProperty("url", "jdbc:mysql://hadoop102:3306/atguigu?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true")
        properties.setProperty("username", "root")
        properties.setProperty("password", "5859660Qq")
        properties.setProperty("maxActive","50")
        val source: DataSource = DruidDataSourceFactory.createDataSource(properties)
        val conn = source.getConnection()
        //val prestatment = conn.prepareStatement("select userid from blacklist where userid = 1")
       // prestatment.close()
        conn.close()

      }
    }

  }
}
