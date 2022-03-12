package api_use

import org.apache.spark.rdd.RDD
import org.apache.spark._

object Review2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
    sc.setCheckpointDir("./ck1")
    val RDD1 = sc.makeRDD(List(("A",1),("A",2),("A",3),("A",4),("B",5),("B",6),("C",7),("B",8),("A",9)),2).partitionBy(new Partitions())
    val RDD2 = sc.makeRDD(List(("A",1),("A",2),("A",3),("A",4),("B",5),("B",6),("C",7),("B",8),("A",9)),4)
    println(RDD1.toDebugString)
    println("----------------------------------------")
    val unit: RDD[(String, (Iterable[Int], Iterable[Int]))] = RDD1.cogroup(RDD2)
    println(unit.toDebugString)
    println("----------------------------------------")
    val unit1 = unit.map {
      case (k, (v1, v2)) => {
        val list1 = v1.toList
        val list2 = v2.toList
        ((k, v1), (k, v2))
      }
    }.partitionBy(new HashPartitioner(2))
    println(unit1.toDebugString)
    println("----------------------------------------")
    val unit2 = unit1.repartition(6)
    println(unit2.toDebugString)
    println("----------------------------------------")
    unit2.cache()
    unit2.checkpoint()
    println(unit2.toDebugString)
    println("-----------------检查点-----------------------")
    val unit3 = unit2.flatMap {
      case ((k1, v1), (k2, v2)) => {
        val a = v1 ++ v2
        a
      }
    }
    println(unit3.toDebugString)
    println("----------------------------------------")
    unit3.collect()
  }
}
class Partitions extends Partitioner{
  override def numPartitions: Int = 3

  override def getPartition(key: Any): Int = {
    if(key.toString=="A"){
      0
    }else if(key.toString=="B"){
      1
    }
    else{
      2
    }
  }
}
