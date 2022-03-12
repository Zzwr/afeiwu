package onClass

object test3 {
  def main(args: Array[String]): Unit = {
    //char int string
    def ch(a:Char): Int=>String=>Boolean ={
      def in(b:Int):String=>Boolean ={
        def st(c:String): Boolean ={
          a=='0'||b==0||c==""
        }
        st _
      }
      in _
    }
    def tt(a:Char)(b:Int)(c:String): Boolean ={
      a=='0'||b==0||c==""
    }
    println(ch('0')(0)(""))
    println(ch('a')(3)("a"))
    println(tt('0')(0)(""))
    println(tt('a')(3)("a"))

  }
}
