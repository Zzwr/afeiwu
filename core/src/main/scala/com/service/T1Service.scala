package com.service

import com.dao.T1Dao
import org.apache.spark.rdd.RDD

class T1Service {
  private val dao = new T1Dao()
  def  dataAnalsis() ={
    val dataRDD = dao.readFile("input/user_visit_action.txt")
    //TODO 点击操作
    val filterRDD = dataRDD.filter(x=>{
      val splits = x.split("_")
      splits(6)!="-1"&&splits(6)!=null
    })
    val splitsRDD = filterRDD.map(x=>{x.split("_")})
    val chick = splitsRDD.map(x=>(x(6),1))
    val chickSum = chick.reduceByKey(_+_)

    //TODO 下单操作
    val filterRDD1 = dataRDD.filter(x => {
      val splits = x.split("_")
      splits(8) != "null"
    })
    val search = filterRDD1.flatMap(x => {
      val splits = x.split("_")
      splits(8).split(",").map((_, 1))
    })
    val searchSum = search.reduceByKey(_+_)

    //TODO 支付
    val filterRDD2 = dataRDD.filter(x=>{
      val splits = x.split("_")
      splits(10)!="null"
    })
    val pay = filterRDD2.flatMap(x => {
      val splits = x.split("_")
      splits(10).split(",").map((_, 1))
    })
    val paySum = pay.reduceByKey(_+_)
    //TODO 聚合
    val groupRDD:RDD[(String,(Iterable[Int],Iterable[Int],Iterable[Int]))] = chickSum.cogroup(searchSum,paySum)
    val resultRDD = groupRDD.mapValues {
      case (itr1, itr2, itr3) => {
        val it1 = itr1.iterator
        var t1 = 0
        while (it1.hasNext) {
          t1 = it1.next()
        }
        val it2 = itr2.iterator
        var t2 = 0
        while (it2.hasNext) {
          t2 = it2.next()
        }
        val it3 = itr3.iterator
        var t3 = 0
        while (it3.hasNext) {
          t3 = it3.next()
        }
        (t1, t2, t3)
      }
    }
    val datas = resultRDD.sortBy(_._2,ascending = false).take(10)
    datas
  }
}
