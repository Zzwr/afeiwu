package api_use

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Review {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
    val RDD1 = sc.makeRDD(List(("A",1),("A",2),("A",3),("A",4),("B",5),("B",6),("C",7),("B",8),("A",9)),2)
    val RDD2 = sc.makeRDD(List((1,3),(1,2),(1,1),(1,1),(1,3)),4)
    RDD1.combineByKey(
      (v:Int)=>"a",
      (c:String,v:Int)=>c+"@"+v,
      (c1:String,c2:String)=>c1+"$"+c2
    ).collect().foreach(println)
    println("--------------------------------------------------------------")
    val tuples = RDD1.mapPartitionsWithIndex(f = (Index, iter) => {
      var part_map = scala.collection.mutable.Map[String, List[(String, Int)]]()
      while (iter.hasNext) {
        var part_name = "part_" + Index
        var elem = iter.next();
        if (part_map.contains(part_name)) {
          var elems = part_map(part_name)
          elems ::= elem
          part_map(part_name) = elems
        } else {
          part_map(part_name) = List[(String, Int)] {
            elem
          }
        }
      }
      part_map.iterator
    }).collect()
    for (elem <- tuples) {
      println(elem._1,"-->",elem._2)
    }
  }
}
