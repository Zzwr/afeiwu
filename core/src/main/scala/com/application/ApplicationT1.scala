package com.application

import com.commons.GApplication
import com.contorller.T1Controller
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ApplicationT1 extends App with GApplication{

  start(){
    val controller = new T1Controller
    controller.dispatch()
  }
}
