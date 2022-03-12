package review

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class User(name:String,age:Int)
object Dtransfer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("conf")
    val session = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._
    val caseClassDS = Seq(User("t1",1),User("t2",2),User("t3",3)).toDS()
    
  }
}
