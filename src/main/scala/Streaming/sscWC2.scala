package Streaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sscWC2 {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf()
      .setAppName("SpStreaming")
      .setMaster("local")

    val ssc = new StreamingContext(conf,Seconds(5))

    //设置检查点
    ssc.checkpoint("D:/co")

    val ds1 =ssc.socketTextStream("clone",8889)
    val tup = ds1.flatMap(_.split(" ")).map((_,1))

    val res = tup.updateStateByKey(func,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    res.print()
    ssc.start()
    ssc.awaitTermination()
  }
  val func = (it:Iterator[(String,Seq[Int],Option[Int])])=>{
    it.map(x =>{
      (x._1,x._2.sum+x._3.getOrElse((0)))
    })
  }
}
