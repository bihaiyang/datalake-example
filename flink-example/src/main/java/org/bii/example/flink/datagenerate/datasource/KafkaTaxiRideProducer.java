package org.bii.example.flink.datagenerate.datasource;

import cn.hutool.json.JSONUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Random;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bii.example.flink.common.datatype.TaxiRide;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/11/03
 */
public class KafkaTaxiRideProducer {
    
    public static final int SLEEP_MILLIS_PER_EVENT = 10;
    private static final int BATCH_SIZE = 5;
    private volatile boolean running = true;
    private final KafkaProducer<String, String> kafkaProducer;
    private String topic;
    
    public KafkaTaxiRideProducer(){
        this("taxi_ride_1", "127.0.0.1:9092");
    }
    
    public KafkaTaxiRideProducer(String topic, String brokerServer){
        this.topic = topic;
        this.kafkaProducer = new KafkaProducer<>(getProp(brokerServer));
    }
    
    public void run() throws Exception {
        
        PriorityQueue<TaxiRide> endEventQ = new PriorityQueue<>(100);
        long id = 0;
        long maxStartTime = 0;
        
        while (running) {
            
            List<TaxiRide> startEvents = new ArrayList<>(BATCH_SIZE);
            for (int i = 1; i <= BATCH_SIZE; i++) {
                TaxiRide ride = new TaxiRide(id + i, true);
                startEvents.add(ride);
                maxStartTime = Math.max(maxStartTime, ride.getEventTimeMillis());
            }
            
            for (int i = 1; i <= BATCH_SIZE; i++) {
                endEventQ.add(new TaxiRide(id + i, false));
            }
            
            while (endEventQ.peek().getEventTimeMillis() <= maxStartTime) {
                TaxiRide ride = endEventQ.poll();
                System.out.println(JSONUtil.toJsonStr(ride));
                this.kafkaProducer.send(new ProducerRecord<>(topic, JSONUtil.toJsonStr(ride)));
            }
            
            java.util.Collections.shuffle(startEvents, new Random(id));
            startEvents.iterator().forEachRemaining(r -> {
                System.out.println(JSONUtil.toJsonStr(r));
                this.kafkaProducer.send(new ProducerRecord<>(topic, JSONUtil.toJsonStr(r)));
            });
            
            id += BATCH_SIZE;

            Thread.sleep(BATCH_SIZE * SLEEP_MILLIS_PER_EVENT);
        }
    }
    
    
    
    private Properties getProp(String broke) {
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
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
    
    public static void main(String[] args) throws Exception {
        new KafkaTaxiRideProducer().run();
    }
}
