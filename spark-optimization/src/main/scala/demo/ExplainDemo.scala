package demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ExplainDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val conf = new SparkConf().setAppName("demo").setMaster("local[*]")
    val sparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    sparkSession.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://hadoop102:8020")
    sparkSession.sql("use sparktuning")
    val sqlstr =
      """
        |select
        |  sc.courseid,
        |  sc.coursename,
        |  sum(sellmoney) as totalsell
        |from sale_course sc join course_shopping_cart csc
        |  on sc.courseid=csc.courseid and sc.dt=csc.dt and sc.dn=csc.dn
        |group by sc.courseid,sc.coursename
      """.stripMargin
    println("====================================================")
    sparkSession.sql(sqlstr).explain("simple")
    println("====================================================")
    sparkSession.sql(sqlstr).explain("extended")
    println("====================================================")
    sparkSession.sql(sqlstr).explain("codegen")
    println("====================================================")
    sparkSession.sql(sqlstr).explain("formatted")
  }
}
