package ym

import com.fasterxml.jackson.module.scala.DefaultScalaModule

class ObjectMapper()

object testTransient {
  @transient private var objectMapper:ObjectMapper = _
  def getInstance():ObjectMapper = {
    if(objectMapper == null){
      objectMapper = new ObjectMapper()
      //objectMapper.registerModule(DefaultScalaModule) //将Java类型的注册为Scala的对象
    }
    objectMapper
  }

  def main(args: Array[String]): Unit = {
    getInstance()
  }
}
