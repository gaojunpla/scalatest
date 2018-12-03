package Kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SStreamKafka extends App {

    val conf = new SparkConf()
      .setAppName("haha")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(10))

    val zk = "master:2181"

    val groupId = "group1"
    val topic = Map[String,Int]("test2" ->1)
    //第一个key值，第二个是写入内容
    val data = KafkaUtils.createStream(ssc,zk,groupId,topic)

    val lines = data.map(_._2)
    val w =lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()

}
