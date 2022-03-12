import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies, LocationStrategy}

object KSConsumer {
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
    //过滤已经存在黑名单
    val filterDF = clickDatas.transform(rdd => {

      val conn = getConn()
      val statement = conn.prepareStatement("select * from blacklist")
      rdd.filter(adclick => {
        val bool = statement.executeQuery().next()
        statement.close()
        conn.close()
        bool
      })
    })
    //1聚合数据
    val mapDF = clickDatas.map(data => {
      val sdf = new SimpleDateFormat("yyyy-mm-dd")
      val day = sdf.format(new java.util.Date(data.curTime.toLong))
      ((day, data.adId, data.userId), 1)
    })
    val resDF = mapDF.reduceByKey(_+_)
    resDF.foreachRDD(
      rdd=>{
        rdd.foreachPartition{
            iter=>{
              val conn = getConn()
              iter.foreach{
              case ((day,adid,uid),count)=>{
                if (count>15){
                  val statement = conn.prepareStatement("insert into blacklist(userid) values(?)  ON DUPLICATE KEY update userid=?")
                  statement.setInt(1,uid.toInt)
                  statement.setInt(2,uid.toInt)
                  statement.execute()
                  statement.close()
                }
                else{
                  val statement = conn.prepareStatement("insert into uc(dt,userid,adid,count) values(?,?,?,?)  ON DUPLICATE KEY update count=count+?")
                  statement.setString(1,day)
                  statement.setInt(2,uid.toInt)
                  statement.setInt(3,adid.toInt)
                  statement.setInt(4,count)
                  statement.setInt(5,count)
                  statement.execute()
                  //判断表中数据是否超过100，超过加黑名单
                  val statement1 = conn.prepareStatement("select count from uc where userid=?")
                  statement1.setInt(1,uid.toInt)
                  val resultSet = statement1.executeQuery()
                  while (resultSet.next()){
                    val cnt = resultSet.getInt(1)
                    if (cnt>15){
                      val statement = conn.prepareStatement("insert into blacklist(userid) values(?)  ON DUPLICATE KEY update userid=?")
                      statement.setInt(1,uid.toInt)
                      statement.setInt(2,uid.toInt)
                      statement.execute()
                      statement.close()
                    }
                  }
                }

              }
            }
              conn.close()
          }
        }









      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
case class adClick(curTime:String,area:String,city:String,userId:String,adId:String)
