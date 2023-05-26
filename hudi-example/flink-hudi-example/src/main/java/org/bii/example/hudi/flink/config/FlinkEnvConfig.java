package org.bii.example.hudi.flink.config;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/19
 */
public class FlinkEnvConfig {
    /**
     * 获取流式执行环境
     *
     * @return
     */
    public static StreamExecutionEnvironment getStreamEnv() {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        return executionEnvironment;
    }
    
    /**
     * 获取流式table执行环境
     *
     * @return
     */
    public static StreamTableEnvironment getStreamTableEnv() {
        StreamExecutionEnvironment streamEnv = getStreamEnv();
        return StreamTableEnvironment.create(streamEnv,
                EnvironmentSettings.newInstance().inStreamingMode().build());
    }
    
    public static TableEnvironment getBatchTableEnv() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    }
}
