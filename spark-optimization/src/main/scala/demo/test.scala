package demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession}
import org.sparkproject.jetty.server.Authentication.User

case class User(id:Int,name:String)
object test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().set("spark.sql.shuffle.partitions", "20").setMaster("local[*]").setAppName("aa")
    val session = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._
    Seq(User(1,"a"),User(2,"b"),User(3,"c")).toDS().createOrReplaceTempView("t1")
    Seq(User(1,"z"),User(2,"f")).toDS().createOrReplaceTempView("t2")
    val frame: DataFrame = session.sql("select t1.id,t1.name n1,t2.name n2 from t1 join t2 on t1.id=t2.id")
    println(frame.rdd.partitions.length)
    frame.write.json("file:///D:\\java\\idea\\IntelliJ IDEA 2018.2.3\\spark\\op")
    while (true){

    }
  }
}
