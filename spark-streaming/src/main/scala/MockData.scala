import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object MockData {
  def main(args: Array[String]): Unit = {
    //生成模拟数据
    //格式：timestamp area city userid adid
    //       时间戳   区域  城市  用户  广告
    //Application=>Kafka=>SparkStreaming=>Analysis
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    val kafkaproducer = new KafkaProducer[String,String](properties)
    while (true){
      val datasList = mockDatas()
      datasList.foreach(data=>{
        kafkaproducer.send(new ProducerRecord[String,String]("ks",data))
        println(data)
      })
      Thread.sleep(3000)
    }
  }
  def mockDatas(): ListBuffer[String] ={
    val areaList = List("华东","华西","华南","华北")
    val cityList = List("上海","北京","大连","沈阳")
    val datas = ListBuffer[String]()
    for (i<- 0 to 30){
        val curtime = System.currentTimeMillis()
        val area = areaList(new Random().nextInt(3))
        val city = cityList(new Random().nextInt(3))
        val userid = new Random().nextInt(6)+1
        val adid = new Random().nextInt(6)+1
        datas+=s"$curtime $area $city $userid $adid"
    }
    datas
  }
}
