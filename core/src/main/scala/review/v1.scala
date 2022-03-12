package review

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
import scala.reflect.ClassTag

object v1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("aa")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)
    val rdd1 = rdd.repartition(3)
    println("======================rdd1")
    rdd1.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)
    val rdd2 = rdd1.map((_, 1))
    println("======================rdd2")
    rdd2.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)
    val rdd3 = rdd2.repartition(2)
    println("======================rdd3")
    rdd3.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)
    val rdd4 = rdd3.reduceByKey(_ + _)
    //val rdd5 = rdd4.sortBy(_)
    println("================================toDebugString")
    println(rdd4.toDebugString)
    println("=====================================")
    println()
    rdd4.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)
    sc.stop()
  }
}
