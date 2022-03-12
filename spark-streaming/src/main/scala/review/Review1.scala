package review

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Review1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("aa").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    //val unit: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",8888)
    val queue = new mutable.Queue[RDD[Int]]()
    val value = ssc.queueStream(queue)
    value.print()
    ssc.start()
    for (i<-1 to 5){
      queue+=ssc.sparkContext.makeRDD(1 to 300)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }
}
