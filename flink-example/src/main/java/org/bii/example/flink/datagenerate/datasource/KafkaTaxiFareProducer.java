package org.bii.example.flink.datagenerate.datasource;

import cn.hutool.json.JSONUtil;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bii.example.flink.common.datatype.TaxiFare;
import org.bii.example.flink.common.utils.DataGenerator;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/11/03
 */
public class KafkaTaxiFareProducer {
    
    private volatile boolean running = true;
    private Instant limitingTimestamp;
    private KafkaProducer<String, String> producer;
    private String topic;
    
    
    public KafkaTaxiFareProducer(){
        this(Duration.ofMinutes(10));
    }
    
    public KafkaTaxiFareProducer(Duration duration){
        this(duration, "taxi_fare_1", "127.0.0.1:9092");
    }
    
    public KafkaTaxiFareProducer(Duration duration,  String topic, String brokerServer){
        this.topic = topic;
        this.limitingTimestamp = DataGenerator.BEGINNING.plus(duration);
        this.producer = new KafkaProducer<>(getProp(brokerServer));
        
    }
    
    public void run() throws InterruptedException {
        long id = 1;
        while (running) {
            TaxiFare fare = new TaxiFare(id);
            // don't emit events that exceed the specified limit
            if (fare.startTime.compareTo(limitingTimestamp) >= 0) {
                break;
            }
            ++id;
            System.out.println(topic + " " + JSONUtil.toJsonStr(fare));
            producer.send(new ProducerRecord<>(topic, JSONUtil.toJsonStr(fare)));
            // match our event production rate to that of the TaxiRideGenerator
            Thread.sleep(TaxiRideGenerator.SLEEP_MILLIS_PER_EVENT);
        }
        producer.close();
    }
    
    private static Properties getProp(String broke) {
        Properties props = new Properties();
        props.put("bootstrap.servers", broke);
        //所有follower都响应了才认为消息提交成功，即"committed"
        props.put("acks", "all");
        //retries = MAX 无限重试，直到你意识到出现了问题
        props.put("retries", 0);
        //batch.size当批量的数据大小达到设定值后，就会立即发送，不顾下面的linger.ms
        //延迟1ms发送，这项设置将通过增加小的延迟来完成--即，不是立即发送一条记录，producer将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理
        props.put("linger.ms", 1);
        //producer可以用来缓存数据的内存大小。
        props.put("buffer.memory", 33554432);
        props.put("auto.create.topics.enable", true);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
    
    
    public void stop(){
        this.running = false;
    }
    
    public static void main(String[] args) throws InterruptedException {
        new KafkaTaxiFareProducer().run();
        
    }
  
}
