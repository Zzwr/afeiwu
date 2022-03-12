package review

import java.io.{BufferedInputStream, BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

class CustomerReceiver(host: String="localhost",port: Int=9999) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  override def onStart(): Unit = {
    val socket = new Socket(host,port)
    var input:String = null
    val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream))

    while (!isStopped()&&((input = reader.readLine()) != null)){
      store(input)
    }
    reader.close()
    socket.close()
    restart("hi")
  }

  override def onStop(): Unit = {

  }
}
object Review2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ttt")
    val ssc = new StreamingContext(conf,Seconds(3))
    val myInputDStream = ssc.receiverStream(new CustomerReceiver())
    myInputDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
