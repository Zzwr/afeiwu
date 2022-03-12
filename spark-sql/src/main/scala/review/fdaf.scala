package review

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession,functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object fdaf {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    spark.udf.register("toRes",functions.udaf(new CityRemarkUDAF()))
    spark.sql(
      """
        |select ci.city_name,ci.area,pi.product_name
        |from user_visit_action uva
        |join city_info ci
        |on uva.city_id=ci.city_id
        |join product_info pi
        |on uva.click_product_id=pi.product_id where uva.click_product_id!=-1
      """.stripMargin).createOrReplaceTempView("t1")
    spark.sql(
      """
        |select t1.area,t1.product_name,count(*) cnt,toRes(city_name) res
        |from t1 group by area,product_name
      """.stripMargin).createOrReplaceTempView("t2")
    spark.sql(
      """
        |select t2.area,t2.product_name,t2.cnt,t2.res,rank() over(partition by area order by cnt) rk from t2
      """.stripMargin).createOrReplaceTempView("t3")
    spark.sql(
      """
        |select * from t3 where rk<=3
      """.stripMargin).show()
    spark.close()
  }
}
case class Buffer(var totalcnt:Long,var cityMap:mutable.Map[String,Long])
class CityRemarkUDAF extends Aggregator[String,Buffer,String] {
  override def zero: Buffer = Buffer(0L,mutable.Map[String,Long]())

  override def reduce(b: Buffer, a: String): Buffer = {
    b.totalcnt+=1
    b.cityMap(a)=b.cityMap.getOrElse(a,0L)+1
    b
  }

  override def merge(b1: Buffer, b2: Buffer): Buffer = {
    b1.totalcnt+=b2.totalcnt
    b2.cityMap.foreach{
      case(city,cityCnt)=>{
        b1.cityMap(city)=b1.cityMap.getOrElse(city,0L)+cityCnt
      }
    }
    b1
  }

  override def finish(reduction: Buffer): String = {
    val resList = ListBuffer[String]()
    val sortList = reduction.cityMap.toList.sortBy(_._2).reverse
    var sum: Long =0L
    sortList.foreach{
      case (city,cnt)=>{
        val res: Long = cnt*100/reduction.totalcnt
        resList.append(city+""+res+"%")
        sum=sum+res
      }
    }
    if (reduction.cityMap.size>2){
      resList.append("其他"+(100-sum)+"%")
    }
    resList.mkString(",")
  }

  override def bufferEncoder: Encoder[Buffer] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}
