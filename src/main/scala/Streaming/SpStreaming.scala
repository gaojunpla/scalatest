package Streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SpStreaming {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf()
      .setAppName("SpStreaming")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //设置数据时间间隔
    val ssc = new StreamingContext(sc,Seconds(10))
    //监听IP端口
    val DS1 =ssc.socketTextStream("clone",1239)
    //
    val DS2 =DS1.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    DS2.print()

    //提交任务到集群
    ssc.start()

    //等待处理下一批次任务
    ssc.awaitTermination()

  }
}
