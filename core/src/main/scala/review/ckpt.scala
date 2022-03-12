package review

import org.apache.spark.{SparkConf, SparkContext}

object ckpt {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("tt").setMaster("local[*]"))
    sc.setCheckpointDir("./ck")
    val rdd1 = sc.makeRDD('a' to 'z')
    val rdd2 = rdd1.map((_,System.currentTimeMillis()))
    rdd2.cache()
    rdd2.checkpoint()
    rdd2.collect().foreach(println)
    println("==============================================")
    rdd2.collect().foreach(println)
    Thread.sleep(1000000)
    sc.stop()
  }
}
