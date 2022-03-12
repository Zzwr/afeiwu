package onClass

import scala.io.StdIn
import scala.util.control.Breaks

object test1 {

  def main(args: Array[String]): Unit = {
    def pow(a:Int,b:Int):Int={
      var n:Int = 1
      for (i<-1 to b){
        n = n*a
      }
      n
    }
    def res(a:Int,b:Int,c:(Int,Int)=>Int):Int={
      c(a,b)

    }
    val value = res(1,2,_+_)
    val func:(Int,Int)=>Int = pow
    val value2 = res(3,2,pow)
    println(value2)



  }
}
