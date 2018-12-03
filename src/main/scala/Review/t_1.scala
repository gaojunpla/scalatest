package Review

import org.apache.spark.{SparkConf, SparkContext}

object t_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("hahaha")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))
    val tup = lines.map(line =>{
      val regex = "^([0-9]),(\\w+),(.+),([0-9.]+),([0-9\\-]+)$"
      line.matches(regex)


    })
    tup.collect().foreach(print)
  }
}
