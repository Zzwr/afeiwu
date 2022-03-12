package review

import groovy.sql.DataSet
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.sparkproject.jetty.server.Authentication.User
case class Person(username:String,age:String)
object Review1 {
  def main(args: Array[String]): Unit = {

    //创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val session = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._
    val frame: DataFrame = session.read.json("core/tables/user.json")
    frame.createOrReplaceTempView("user")
    val rdd1: RDD[(Int, String, Int)] =
      session.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",28),(3,"wangwu",
        20)))
    val unit: Dataset[Person] = frame.as[Person]
    unit.select("*").show()
    val frame1: DataFrame = unit.toDF()
    val rdd: RDD[Person] = unit.rdd
    rdd1.toDF()
    val unit3: Dataset[(Int, String, Int)] = rdd1.toDS()
  }
}
