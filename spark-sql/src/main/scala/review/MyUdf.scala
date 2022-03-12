package review

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object MyUdf {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val session = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._
    val frame: DataFrame = session.read.format("json").load()
    frame.rdd
    frame.createOrReplaceTempView("usr")
    session.udf.register("avgAge",functions.udaf(new MyAvgUAF1))
    session.sql("select avgAge(age) from usr group by username").show()
  }
}
case class User(var username:String,var age:Int)
case class AgeBuffer(var sum:Int,var count:Int)
class MyAvgUAF1 extends Aggregator[User,AgeBuffer,Double]{
  override def zero: AgeBuffer = AgeBuffer(0,0)

  override def reduce(b: AgeBuffer, a: User): AgeBuffer = {
    b.count+=1
    b.sum+=a.age
    b
  }

  override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
    b1.sum+=b2.sum
    b1.count+=b2.count
    b1
  }

  override def finish(reduction: AgeBuffer): Double = {
    println(reduction.sum)
    println(reduction.count)
    reduction.sum.toDouble/reduction.count
  }

  override def bufferEncoder: Encoder[AgeBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
