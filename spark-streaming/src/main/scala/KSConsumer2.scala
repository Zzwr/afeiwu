import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object KSConsumer2 {
  def getConn(): Connection ={
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://hadoop102:3306/atguigu?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true")
    properties.setProperty("username", "root")
    properties.setProperty("password", "5859660Qq")
    properties.setProperty("maxActive","1000")
    val source: DataSource = DruidDataSourceFactory.createDataSource(properties)
    source.getConnection
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    val confMap:Map[String,Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"hadoop102:9092",
      ConsumerConfig.GROUP_ID_CONFIG->"atguigu",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->"org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->"org.apache.kafka.common.serialization.StringDeserializer"
    )
    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] =KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Set("ks"),kafkaParams = confMap)
    )
    val clickDatas = kafkaDataDS.map(data => {
      val splits = data.value().split(" ")
      adClick(splits(0), splits(1), splits(2), splits(3), splits(4))
    })
    val mapDS = clickDatas.map(data => {
      val day = new SimpleDateFormat("yyyy-mm-dd").format(new Date(data.curTime.toLong))
      ((day, data.adId, data.city, data.area), 1)
    })
    val resDS = mapDS.reduceByKey(_+_)
    resDS.foreachRDD{
      rdd=>{
        rdd.foreachPartition(
          iter=>{
            val conn = getConn()
            iter.foreach{
              case ((day, adId, city, area), count)=>{
                //jdbc谁爱写谁写去
              }
            }
          }
        )
      }
    }
  }
}
//case class adClick(curTime:String,area:String,city:String,userId:String,adId:String)