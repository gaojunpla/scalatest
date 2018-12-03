package Kafka;


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CoustomC {
    //指定消费主题
    private static String topic = "test";
    //指定消费者线程个数
    private static int Threads = 2;

    public static void main(String[] args) {
        //配置消费者连接
        Properties prop = new Properties();
        //指定zookeeper元数据
        prop.put("zookeeper.connect","master:2181,clone:2181,clone2:2181");
        //指定消费者组
        prop.put("group.id","testGroup");
        //配置消费消息开始位置,从偏移量最小位置开始消费，也可以用largest开始消费
        prop.put("auto.offset.reset","smallest");

        //
        ConsumerConfig config = new ConsumerConfig(prop);

        //创建消费者
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
        //创建map保存topic信息
        HashMap<String, Integer> map = new HashMap<>();
        map.put(topic,Threads);

        //创建信息流
        Map<String, List<KafkaStream<byte[],byte[]>>> comsumemap =consumer.createMessageStreams(map);
        List<KafkaStream<byte[],byte[]>> kafkaStreams = comsumemap.get(topic);

        //循环kafka拉取信息
        for (final KafkaStream<byte[],byte[]> kafkaStream:kafkaStreams){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (MessageAndMetadata<byte[],byte[]> msg :kafkaStream){
                        String message = new String(msg.message());
                        System.out.println(message);
                    }
                }
            }).start();
        }



    }
}
