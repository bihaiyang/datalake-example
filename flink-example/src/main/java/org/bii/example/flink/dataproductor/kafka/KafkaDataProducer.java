package org.bii.example.flink.dataproductor.kafka;

import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/29
 */
public class KafkaDataProducer {
    
    public static void main(String[] args) {
        
        String host = "127.0.0.1:9092";
        String topic = "producer_test";
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(getProp(host));
        Scanner scanner = new Scanner(System.in);
        while (true){
            System.out.println("data: ->");
            String data = scanner.nextLine();
            if(data == null){
                continue;
            }
            if("exit".equals(data)){
                break;
            }
            kafkaProducer.send(new ProducerRecord<>(topic, data));
        }
        kafkaProducer.close();
    }
    
    
    private static Properties getProp(String zk) {
        Properties props = new Properties();
        props.put("bootstrap.servers", zk);//xxx服务器ip
        props.put("acks", "all");//所有follower都响应了才认为消息提交成功，即"committed"
        props.put("retries", 0);//retries = MAX 无限重试，直到你意识到出现了问题
        //batch.size当批量的数据大小达到设定值后，就会立即发送，不顾下面的linger.ms
        //延迟1ms发送，这项设置将通过增加小的延迟来完成--即，不是立即发送一条记录，producer将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);//producer可以用来缓存数据的内存大小。
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
