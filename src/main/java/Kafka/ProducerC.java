package Kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class ProducerC {
    public static void main(String[] args) {
        //配置producer信息
        Properties prop = new Properties();
        //要发送给哪儿
        prop.put("metadata.broker.list","master:9092,clone:9092,clone2:9092");
        //序列化方式
        prop.put("serializer.class","kafka.serializer.StringEncoder");
        //配置信息包装
        ProducerConfig config = new ProducerConfig(prop);

        //创建producer
        Producer<String, String> producer = new Producer<String, String>(config);
        //
        for (int i = 0; i < 100; i++) {
            producer.send(new KeyedMessage<String, String>("test","message"+i));
        }

    }
}
