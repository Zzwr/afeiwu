package review.com.one

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object demo2 {
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
  val flatRdd: RDD[(String,String,Int)] = actionRDD.flatMap(
    data => {
      if (data.click_category_id != "-1") {
        List((data.click_category_id, data.session_id, 1))
      }
      else if (data.order_category_ids != null) {
        data.order_category_ids.split(",").map(dt => (dt, data.session_id, 1))
      }
      else if (data.pay_category_ids != null) {
        data.pay_category_ids.split(",").map(dt => (dt, data.session_id, 1))
      }
      else {
        Nil
      }
    })

}
