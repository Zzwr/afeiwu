package review.com.one

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import review.com.one.demo2.actionRDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

//点击--下单--支付
object demo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("input/user_visit_action.txt")
    val actionRDD: RDD[UserVisitAction] = rdd.map(
      line => {
        val datas: Array[String] = line.split("_")
        //将解析出来的数据封装到样例类里面
        UserVisitAction(
          datas(0),
          datas(1),
          datas(2),
          datas(3),
          datas(4),
          datas(5),
          datas(6),
          datas(7),
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12)
        )
      }
    )
    val flatRDD: RDD[(String, (String, Int, Int, Int))] = actionRDD.flatMap(data => {
      if (data.click_category_id!="-1"){
        List((data.click_category_id,(data.click_category_id,1,0,0)))
      }
      else if (data.order_category_ids!=null){
        val str = data.order_category_ids.split(",")
        str.map(data=>{
          (data,(data,0,1,0))
        })
      }
      else if (data.pay_category_ids!=null){
        val str = data.pay_category_ids.split(",")
        str.map(data=>{
          (data,(data,0,0,1))
        })
      }
      else {
        Nil
      }
    })
    val reduceRDD = flatRDD.reduceByKey(
      (d1, d2) => {
        (d1._1
          , d1._2.toInt + d2._2.toInt
          , d1._3.toInt + d2._3.toInt
          , d1._4.toInt + d2._4.toInt)
      }
    )
    val res: Array[CategoryCountInfo] = reduceRDD.map(data=>{
      (data._1,(data._2._2,data._2._3,data._2._4))
    }).sortBy(_._2,false).take(10).map(data=>{
      CategoryCountInfo(data._1,data._2._1,data._2._2,data._2._3)
    })
    val list:mutable.ListBuffer[String] = new ListBuffer[String]()
    for (i<-res){
      list.append(i.categoryId)
    }
    println(res.foreach(println))
    println("=================================================demo2")
    val flatRdd: RDD[(String,String)] = actionRDD.flatMap(
      data => {
        if (data.click_category_id != "-1") {
          List((data.click_category_id, data.session_id))
        }
        else if (data.order_category_ids != null) {
          data.order_category_ids.split(",").map(dt => (dt, data.session_id))
        }
        else if (data.pay_category_ids != null) {
          data.pay_category_ids.split(",").map(dt => (dt, data.session_id))
        }
        else {
          Nil
        }
      })
    val filterRdd = flatRdd.filter(data=>{list.contains(data._1)})
    filterRdd.map(data=>{
      (data._1+"-"+data._2,1)
    }).reduceByKey(_+_).map(data=>{
      (data._1.split("-")(0),(data._1.split("-")(1),data._2))
    }).groupByKey().mapValues(iter=>{
      iter.toList.sortBy(_._2).reverse.take(10)
    }).foreach(println)
    println("=================================================demo3")
    val fm: Map[String, Int] = actionRDD.map(data=>{(data.page_id,1)}).reduceByKey(_+_).collect().toMap
    val fz1 = actionRDD.map(data=>{(data.session_id,(data.date,data.page_id))})
                      .groupByKey().mapValues(
                                    iter=>{
                                      val value = iter.toList.sortBy(_._1)
                                      val dts = value.map(_._2)
                                      dts.zip(dts.tail)
                                    })
    val fz2 = fz1.flatMap(data => {
      data._2
    }).map(data=>(data,1)).reduceByKey(_ + _)
    val res3 = fz2.map {
      case (p2p, cnt) => {
        (p2p, cnt.toInt / fm.getOrElse(p2p._1, 0).toDouble)
      }
    }
    res3.foreach(println)
    sc.stop()
  }
}
// 输出结果表
case class CategoryCountInfo(var categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long)//支付次数

//用户访问动作表
case class UserVisitAction(date: String,//用户点击行为的日期
                           user_id: String,//用户的ID
                           session_id: String,//Session的ID
                           page_id: String,//某个页面的ID
                           action_time: String,//动作的时间点
                           search_keyword: String,//用户搜索的关键词
                           click_category_id: String,//某一个商品品类的ID11
                           click_product_id: String,//某一个商品的ID
                           order_category_ids: String,//一次订单中所有品类的ID集合111
                           order_product_ids: String,//一次订单中所有商品的ID集合
                           pay_category_ids: String,//一次支付中所有品类的ID集合11
                           pay_product_ids: String,//一次支付中所有商品的ID集合
                           city_id: String)//城市 id