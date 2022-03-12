package api_use

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Review3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
    val rdd = sc.makeRDD(List("hello","hello","spark"))
    var wcAcc = new MyAcc()
    sc.register(wcAcc,"wc")
    rdd.foreach(
      word=>wcAcc.add(word)
    )
    println(wcAcc.value)
  }
}
class MyAcc extends AccumulatorV2[String,mutable.Map[String,Int]]{
  private var map:mutable.Map[String, Int] = mutable.Map()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new MyAcc

  override def reset(): Unit = map.clear()

  override def add(key: String): Unit = {
//    var num = map.getOrElse(key,0)
//    num+=1
//    map.update(key,num)
      map(key) = map.getOrElse(key,0)+1
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    val map1 = map
    val map2 = other.value
    map = map1.foldLeft(map2)(
      (m,kv)=>{
        m(kv._1) = m.getOrElse(kv._1,0)+kv._2
        m
      }
      )
  }

  override def value: mutable.Map[String, Int] = map
}
