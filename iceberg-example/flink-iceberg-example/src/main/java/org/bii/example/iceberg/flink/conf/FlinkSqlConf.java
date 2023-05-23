package org.bii.example.iceberg.flink.conf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/22
 */
public class FlinkSqlConf {
    
    
    
    
    public static StreamTableEnvironment getEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(configuration())
                .build();
        
        return StreamTableEnvironment.create(env, settings);
    }
    
    
    public static StreamTableEnvironment getBatchEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
           EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .withConfiguration(configuration())
                .build();
        
        return StreamTableEnvironment.create(env, settings);
        
    }
    
    private static Configuration configuration(){
    
        Configuration configuration = new Configuration();
        configuration.setString("fs.hdfs.hadoopconf",
                "/Users/bihaiyang/IdeaProjects/github-workspace/datalake-example/iceberg-example/flink-iceberg-example/src/main/resources/");
        return configuration;
    }
    
}
