import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

/*

* 2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17 00:00:02_手机_-1_-1_null_null_null_null_3
2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_48_2019-07-17 00:00:10_null_16_98_null_null_null_null_19
2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_6_2019-07-17 00:00:17_null_19_85_null_null_null_null_7
2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_29_2019-07-17 00:00:19_null_12_36_null_null_null_null_5
2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_22_2019-07-17 00:00:28_null_-1_-1_null_null_15,1,20,6,4_15,88,75_9
2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_11_2019-07-17 00:00:29_苹果_-1_-1_null_null_null_null_7
2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_24_2019-07-17 00:00:38_null_-1_-1_15,13,5,11,8_99,2_null_null_10
2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_24_2019-07-17 00:00:48_null_19_44_null_null_null_null_4
2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_47_2019-07-17 00:00:54_null_14_79_null_null_null_null_2
2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_27_2019-07-17 00:00:59_null_3_50_null_null_null_null_26
2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_27_2019-07-17 00:01:05_i7_-1_-1_null_null_null_null_17
2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_24_2019-07-17 00:01:07_null_5_39_null_null_null_null_10
*
* */
//TODO：本项目需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数
//排名前10

case class UserVisitAction(
                            //date: String,//用户点击行为的日期
                            user_id: Long,//用户的 ID
                            session_id: String,//Session 的 ID
                            //page_id: Long,//某个页面的 ID
                            //action_time: String,//动作的时间点
                            //search_keyword: String,//用户搜索的关键词
                            click_category_id: Long,//某一个商品品类的 ID
                            //click_product_id: Long,//某一个商品的 ID
                            order_category_ids: String,//一次订单中所有品类的 ID 集合
                            //order_product_ids: String,//一次订单中所有商品的 ID 集合
                            pay_category_ids: String,//一次支付中所有品类的 ID 集合
                            //pay_product_ids: String,//一次支付中所有商品的 ID 集合
                            //city_id: Long//城市id
                          )

//（品类，点击总数）（品类，下单总数）（品类，支付总数）
//6, 8, 10
object Spark_Core_Demo {

  var sc: SparkContext = init()
  val dataRDD: RDD[String] = loadDatas()

  //TODO 构建上下文对象
  def init(): SparkContext = new SparkContext(new SparkConf().setAppName("demo1").setMaster("local[*]"))
  //TODO 加载文件到RDD
  def loadDatas(): RDD[String] =  sc.textFile("input/user_visit_action.txt")
  //TODO 关闭连接
  def close(): Unit = sc.stop()

  def main(args: Array[String]): Unit = {
    eg3_type1()
  }
  //统计前十商品id
  def eg1_type1(): Unit ={
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
    datas.foreach(println)
  }
  def eg1_type2(): Unit ={


    var rdd3 = dataRDD.filter(x=>{
      x.split("_")(6)!="-1"
    }).map(x=>{
      val splits = x.split("_")
      (splits(6),(1,0,0))
    })
    var rdd2 = dataRDD.filter(x=>{
      x.split("_")(8)!="null"
    })flatMap(x=>{
      val splits = x.split("_")
      val spt = splits(8).split(",")
      spt.map(x=>(x,(0,1,0)))
    })
    var rdd1 = dataRDD.filter(x=>{
      x.split("_")(10)!="null"
    }).flatMap(x=>{
      val splits = x.split("_")
      val spt = splits(10).split(",")
      spt.map(x=>(x,(0,0,1)))
    })
    val unionRDD = rdd1.union(rdd2).union(rdd3)
    val resRDD = unionRDD.reduceByKey{ (x,y)=>{(x._1+y._1,x._2+y._2,x._3+y._3)}}
    resRDD.sortBy(x=>x._2,ascending = false).collect().foreach(println)
  }
  def eg1_type3(): Array[String] = {
    val resultrdd = dataRDD.flatMap { data => {
      val splits = data.split("_")
      if (splits(6) != "-1") List((splits(6), (1, 0, 0)))
      else if (splits(8) != "null") {
        val spt = splits(8).split(",")
        spt.map(x => (x, (0, 1, 0)))
      }
      else if (splits(10) != "null") {
        val spt = splits(10).split(",")
        spt.map(x => (x, (0, 0, 1)))
      }
      else Nil
    }
    }
    val resRDD = resultrdd.reduceByKey { case (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)}
    val tuples = resRDD.sortBy(_._2, ascending = false) take(10)
    tuples.map(x=>x._1)
  }




  def eg1_type4(): Unit ={
    val resultrdd = dataRDD.flatMap { data => {
      val splits = data.split("_")
      if (splits(6) != "-1") List((splits(6), (1, 0, 0)))
      else if (splits(8) != "null") {
        val spt = splits(8).split(",")
        spt.map(x => (x, (0, 1, 0)))
      }
      else if (splits(10) != "null") {
        val spt = splits(10).split(",")
        spt.map(x => (x, (0, 0, 1)))
      }
      else Nil
    }
    }
    val resRDD = resultrdd.reduceByKey { case (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)}
    val tuples = resRDD.sortBy(_._2, ascending = false) take(10)
    tuples.map(x=>x._1)
  }
  case class HotCagegory(var cid:String,var click:Int,var order:Int,var pay:Int)
  class Acc extends AccumulatorV2[(String,String),mutable.Map[String,HotCagegory]]{
    private val hcmap = mutable.Map[String,HotCagegory]()

    override def isZero: Boolean = hcmap.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCagegory]] = new Acc

    override def reset(): Unit = hcmap.clear()

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val cagegory = hcmap.getOrElse(cid,HotCagegory(cid,0,0,0))
      if(actionType=="click"){
        cagegory.click+=1
      }
      else if(actionType=="order"){
        cagegory.order+=1
      }
      else if(actionType=="pay"){
        cagegory.pay+=1
      }
      hcmap.update(cid,cagegory)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCagegory]]): Unit = {
      val map1 = this.hcmap
      val map2 = other.value
      map2.foreach{
        case (cid,hc)=>{
          val cagegory = map1.getOrElse(cid,HotCagegory(cid,0,0,0))
          cagegory.click+=hc.click
          cagegory.pay+=hc.pay
          cagegory.order+=hc.order
        }
        }
    }

    override def value: mutable.Map[String, HotCagegory] = hcmap
  }







  def eg2_type1(): Unit = {
    //2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_27_2019-07-17 00:01:05_i7_-1_-1_null_null_null_null_17
    //2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_24_2019-07-17 00:01:07_null_5_39_null_null_null_null_10
    val top = eg1_type3()
    val RDD1 = dataRDD.filter(x=>{
      val splits = x.split("_")
      splits(6)!="-1"&&top.contains(splits(6))
    })
    val splitRDD2= RDD1.map(
      x=>{
        val splits = x.split("_")
        ((splits(6),splits(1)),1)
      })
      val resRDD1 = splitRDD2.reduceByKey(_+_)
      val groupRDD = resRDD1.map(x=>(x._1._1,(x._1._2,x._2))).groupByKey()
      //(品类：string,(session,count))
    val resultRDD = groupRDD.map {
      datas => {
        val iter = datas._2.iterator
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    }
    resultRDD.collect().foreach(println)
  }
  def eg3_type1(): Unit ={
    //求分母
    val mapRDD = dataRDD.map {
      datas => {
        val splits = datas.split("_")
        (splits(2), (splits(3), splits(4)))
      }
    }
    val groupRDD = mapRDD.groupByKey()
    val sessionRDD = groupRDD.mapValues {
      datas => {
        datas.toList.sortBy(_._2).map(x=>x._1)
      }
    }
    val fmMapRDD = sessionRDD.flatMap {
      datas => {
        val list: List[String] = datas._2
        val newList: ListBuffer[((String, String), Int)] = ListBuffer()
        for (i <- list.indices) {
          if (i < list.length - 1) newList.+=(((list(i), list(i + 1)), 1))
        }
        newList.toList
      }
    }
    val fmResult = fmMapRDD.reduceByKey(_+_)
    val fmResult2 = fmResult.map(x=>(x._1._1,(x._1._2,x._2)))
    //(sessionID,List(pageID*)
    //求分子
    val fzRDD1 = dataRDD.map {
      datas => {
        val splits = datas.split("_")
        (splits(3), 1)
      }
    }
    val fzRDD2 = fzRDD1.reduceByKey(_+_)

    //分子分母通过key聚合
    val groupRDD2: RDD[(String, ((String, Int), Option[Int]))] = fmResult2.leftOuterJoin(fzRDD2)
    val resRDD = groupRDD2.map {
      case (k1, ((k2, v1), v2)) => {
        ((k1, k2), v1.toDouble / v2.getOrElse(0))
      }
    }
    resRDD.collect().foreach(println)
  }
}
