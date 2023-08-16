package org.bii.example.flink.dataproductor.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/29
 */
public class KafkaDataConsumer implements Runnable{
    
    private final KafkaConsumer<String, String> consumer;
    private ConsumerRecords<String, String> msgList;
    private static String topic;
    private static String brokers;
    private static String groupId;
    
    public KafkaDataConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(topic));
    }
    
    @Override
    public void run() {
        System.out.println(String.format("消费 %s 组 %s topic 数据", groupId, topic));
        try{
            while (true){
                msgList = consumer.poll(Duration.ofSeconds(2));
                if(msgList != null && msgList.count() > 0){
                    msgList.forEach(record -> {
                        System.out.println(record.value());
                    });
                }else {
                    Thread.sleep(1000);
                }
    
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }
    
    public static void main(String[] args) {
        topic = "producer_test";
        groupId = "consumer_1";
        brokers = "127.0.0.1:9092";
        KafkaDataConsumer kafkaDataConsumer = new KafkaDataConsumer();
        Thread thread = new Thread(kafkaDataConsumer);
        thread.start();
    }
    
}
