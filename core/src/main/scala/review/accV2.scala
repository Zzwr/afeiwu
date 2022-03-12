package review

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object accV2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("qnmb"))
    val rdd = sc.makeRDD(List("aa","aa","aa","bb","bb"))
    val acc = new myAcc()
    sc.register(acc)
    rdd.foreach(acc.add(_))
    println(acc.value)
  }
}
class myAcc extends AccumulatorV2[String,mutable.Map[String,Int]] {
  val map:mutable.Map[String,Int] = new mutable.HashMap[String,Int] {}
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new myAcc()

  override def reset(): Unit = map.clear()

  override def add(word: String): Unit = {
    map(word) = map.getOrElse(word,0)+1
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    val map1 = other.value
    map1.foreach{
      case (word,cnt)=>{
        map(word) = map.getOrElse(word,0)+cnt
      }
    }
  }

  override def value: mutable.Map[String, Int] = map
}
