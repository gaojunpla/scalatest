package Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Kafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(3))
    //接下来编写kafka的配置信息
    //首先是kafka依赖的zk信息
    val zks = "192.168.10.131:2181,192.168.10.130:2181,192.168.10.133:2181"
    //kafka消费者组
    val groupId = "gp1"

    //kafka的topic名字，第一个参数是topic名字，第二个参数是线程数
    val topics = Map[String,Int]("test1"->1)
    //创建kafka的输入数据流，来获取kafka中的数据
    val data = KafkaUtils.createStream(ssc,zks,groupId,topics)
    //获取到的数据个是键值对的格式，（key，value）
    //接下来就开始处理kafka中的数据
    val lines = data.flatMap(_._2.split(" "))
    val words = lines.map((_,1))
    val result = words.reduceByKey(_+_)
    //上面的处理逻辑和代码实现和Streaming一样，因为都是统计单词实现WC
    result.print()
    //启动程序
    ssc.start()
    //等待停止
    ssc.awaitTermination()
  }

}
