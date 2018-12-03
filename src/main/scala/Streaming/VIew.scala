package Streaming

import org.apache.spark.SparkConf

import org.apache.spark.streaming.{Seconds, StreamingContext}

object VIew {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf()
      .setMaster("local")
      .setAppName("VIew")

    val ssc = new StreamingContext(conf,Seconds(5))

    //
    ssc.checkpoint("D:\\atmp\\catch\\ck1")
    //获取数据
    val ds = ssc.socketTextStream("clone",1239)
    //调用窗口函数聚合多个批次数据
    val res0 = ds.flatMap(_.split(" ")).map((_,1))

    val res = res0.reduceByKeyAndWindow((x:Int,y:Int)=>{x+y},Seconds(15),Seconds(10))

    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
