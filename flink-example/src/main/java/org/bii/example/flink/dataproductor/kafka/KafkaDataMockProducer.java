package org.bii.example.flink.dataproductor.kafka;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/29
 */
public class KafkaDataMockProducer {
    
    public static void main(String[] args) {
        KafkaProducer<String, String> producer;
        String topic = "producer_test";
        String zk = "127.0.0.1:9092";
        producer = new KafkaProducer<>(getProp(zk));
    
        for (int i = 0; i < 10; i++) {
            Order order = Order.builder()
                    .uuid(UUID.randomUUID().toString())
                    .cityId(RandomUtil.randomLong(1,6))
                    .sex(RandomUtil.randomEle(ListUtil.toList("男","女")))
                    .productId(RandomUtil.randomLong(1,100))
                    .orderTime(DateFormatUtils.format(new Date(),"yyyy-MM-dd HH:mm:ss"))
                    .build();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(new ProducerRecord<>(topic, JSONUtil.toJsonStr(order)));
            System.out.println("Success:"+JSONUtil.toJsonStr(order));
        }
    
        producer.close();
    
    
    }
    
    private static Properties getProp(String broke) {
        Properties props = new Properties();
        props.put("bootstrap.servers", broke);//xxx服务器ip
        props.put("acks", "all");//所有follower都响应了才认为消息提交成功，即"committed"
        props.put("retries", 0);//retries = MAX 无限重试，直到你意识到出现了问题
        //batch.size当批量的数据大小达到设定值后，就会立即发送，不顾下面的linger.ms
        props.put("linger.ms", 1);//延迟1ms发送，这项设置将通过增加小的延迟来完成--即，不是立即发送一条记录，producer将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理
        props.put("buffer.memory", 33554432);//producer可以用来缓存数据的内存大小。
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
    
}
