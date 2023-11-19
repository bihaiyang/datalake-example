package com.bii.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.Properties;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/11/02
 */
public class EmbeddedKafkaStart {
    
    public static void main(String[] args) throws JsonProcessingException {
        
        Properties brokerProps = new Properties();
        // 指定 Kafka 服务器端口
        brokerProps.setProperty("bootstrap.servers", "localhost:9092");
        // 存储日志文件的目录
        brokerProps.setProperty("log.dirs",  System.getProperty("user.dir") + "/tmp/kafka-logs");
        // 指定 ZooKeeper 连接地址
        brokerProps.setProperty("zookeeper.connect", "localhost:2181");
        brokerProps.setProperty("auto.create.topics.enable", "true");
        int[] brokerPorts = {9092};
        EmbeddedKafkaCluster embeddedKafkaCluster = new EmbeddedKafkaCluster(1, brokerPorts, brokerProps);
        
        // 启动 Kafka 服务器
        embeddedKafkaCluster.start();
        
        
    
    }
}
