package com.commons

import com.contorller.T1Controller
import com.util.EnvUtil
import jdk.internal.util.EnvUtils
import org.apache.spark.{SparkConf, SparkContext}

trait GApplication {
  def start(master:String="local[*]",app: String="demo")(loadController: =>Unit): Unit ={
    val sc = new SparkContext(new SparkConf().setAppName(app).setMaster(master))
    EnvUtil.put(sc)
    try{
      loadController
    }catch {
      case ex: Throwable => println(ex.getMessage)
    }
    sc.stop()
  }
}
