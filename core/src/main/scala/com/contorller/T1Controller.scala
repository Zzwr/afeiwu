package com.contorller

import com.service.T1Service
import org.apache.spark.rdd.RDD

class T1Controller {
  private val service = new T1Service()

  def dispatch(): Unit ={
    val datas = service.dataAnalsis()
    datas.foreach(println)
  }
}
