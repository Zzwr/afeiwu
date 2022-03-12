package api_use

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Review4 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
    val rdd:RDD[Any] = sc.makeRDD(List((1,2),(1,2),(1,2)))
    rdd.reduce((x,y)=>3)
    val intToInt = mutable.Map(1->2,2->3)
    val intToInt2 = mutable.Map(1->2,2->3)
    val ints = List("a","b")

  }
}
