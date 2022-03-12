package review

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}

object Review3 {
  def main(args: Array[String]): Unit = {
    val map = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"hadoop102:9092" ,
      ConsumerConfig.GROUP_ID_CONFIG->"atguiguGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->"org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG-> classOf[StringDeserializer]
    )
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Durations.seconds(3))
    val dStream1: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Seq("testTopic"),map))
    dStream1.map(record=>record.value()).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    .print()
    ssc.start()
    ssc.awaitTermination()
  }
}
