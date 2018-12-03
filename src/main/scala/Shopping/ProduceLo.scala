//package Shopping
//
//import kafka.serializer.StringDecoder
//import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
//import org.I0Itec.zkclient.ZkClient
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
//import org.apache.spark.streaming.{Duration, StreamingContext}
//import redis.clients.jedis.Jedis
//
//object ProduceLo {
//  /**
//    * 数据源：D:/atmp/input/order.log
//    * ./kafka-console-producer.sh --broker-list mini1:9092 --topic t2
//    * 要求：
//    * 计算成交总金额
//    * 计算分类下成交总金额
//    * 计算每个区域成交总金额
//    *
//    * @param args
//    */
//  def main(args: Array[String]) = {
//
//    val conf = new SparkConf()
//      .setMaster("local[*]")
//      .setAppName("Shop")
//    val ssc = new StreamingContext(conf,Duration(5000))
//    val topic = "coco"
//
//    val jedis = new Jedis("master",6379)
//
//    //准备kafka的参数
//    val kafkaParams = Map(
//      "metadata.broker.list" -> "master:9092",
//      "group.id" -> "gao",
//      //从头开始读取数据
//      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
//    )
//
//    println(">>>>1")
//    val topics = Set(topic)
//    val kfkaStm = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
//
//    var offset = Array[OffsetRange]()
//    val transform = kfkaStm.transform(rdd =>{
//      offset = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd
//    })
//    val msg = transform.map(_._2)
//
//    msg.foreachRDD(rdd => {
//      rdd.foreachPartition(p =>{
//        p.foreach(x0=>{
//          val x =x0.split(" ")
//          jedis.incrBy("countAll",x(4).toInt)
//          jedis.incrBy(x(2),x(4).toInt)
//          jedis.incrBy(x(1),x(4).toInt)
//        })
//      })
//    }
//      )
//    for(m <- offset){
//      val zkPath = s"${zkTopicDir.consumerOffsetDir}/${m.partition}"
//
//      ZkUtils.updatePersistentPath(zkClient,zkPath,m.untilOffset.toString)
//    }
//
//    val countA =jedis.get("countAll")
//    println(countA)
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
