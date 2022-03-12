package com.dao

import com.util.EnvUtil
import org.apache.spark.rdd.RDD

class T1Dao {
  def readFile(path : String) ={
    val sc = EnvUtil.take()
    val dataRDD: RDD[String] = sc.textFile(path)
    dataRDD
  }
}
