package review

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext, StreamingContextState}

class Review4 {
  def updateFunc = (seq:Seq[Int],state:Option[Int]) => {
    //获取当前批次单词的和
    val currentCount: Int = seq.sum
    //获取历史状态的数据
    val stateCount: Int = state.getOrElse(0)
    //将当前批次的和加上历史状态的数据和,返回
    Some(currentCount + stateCount)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Durations.seconds(3))
    val dStream1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val unit: DStream[(String, Int)] = dStream1.map((_,1))
//    unit.updateStateByKey((seq:Seq[Int],state:Option[Int]) => {
//      //获取当前批次单词的和
//      val currentCount: Int = seq.sum
//      //获取历史状态的数据
//      val stateCount: Int = state.getOrElse(0)
//      //将当前批次的和加上历史状态的数据和,返回
//      Some(currentCount + stateCount)})
    val dStream = dStream1.window(Durations.seconds(3),Durations.seconds(12))

  }
}
