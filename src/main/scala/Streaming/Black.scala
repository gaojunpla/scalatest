package Streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Black {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SpStreaming")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

        //设置数据时间间隔
        val ssc = new StreamingContext(sc,Seconds(10))

        //定义黑名单
        val blackList = List(("tom",true),("jerry",true))
        val blackrdd = sc.parallelize(blackList)

        //收到数据流
        val data = ssc.socketTextStream("clone",1239)
        //解析用户信息
        val usr = data.map(x =>(x.split(" ")(1),x))
        val f1 = usr.transform(u=>{
          val joinRDD = u.leftOuterJoin(blackrdd)
          val f2 =joinRDD.filter(t =>{
            if(t._2._2.getOrElse(false))
              false
            else
              true
          })
          f2.map(_._2._1)
        })

        f1.print()
        ssc.start()

        ssc.awaitTermination()
      }

}
