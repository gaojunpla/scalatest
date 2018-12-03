package join

import org.apache.spark.{SparkConf, SparkContext}

object Ip2o {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Ip2o")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val IP = sc.textFile(args(0))

    val tup = IP.map(line =>{
      val f =line.split("[|]")
      val start =f(2)
      val stop =f(3)
      val pro =f(6)

      (start,stop,pro)
    })
    //先将数据搜集到Driver端
    val tpc = tup.collect()
    //广播
    val boradIP =sc.broadcast(tpc)

    //处理日志
    val log = sc.textFile(args(1))

    val tup2 = log.map(line1=>{
      val f = line1.split("\\|")
      val ip =f(1)
//      val longip =unti
    })


  }
}
