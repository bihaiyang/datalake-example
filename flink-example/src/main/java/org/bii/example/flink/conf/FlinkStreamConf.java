package org.bii.example.flink.conf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/08/04
 */
public class FlinkStreamConf {
    
    
    public static StreamExecutionEnvironment getEnv(){
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
    
    public static StreamExecutionEnvironment getWebUiEnv(){
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
   
    }
    

}
