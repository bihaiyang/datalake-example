package com.bii.oneid.app;

import com.bii.oneid.configuration.Configuration;
import com.bii.oneid.configuration.OneIdOptions;
import com.bii.oneid.util.KeyPriorityUtil;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/10/07
 */
public class OneIdSplitApp {
    
    
    public static void main(String[] args) {
        Configuration config = new Configuration();
        SparkSession spark = SparkSession.builder()
                .appName("OneId graph component connect")
                .getOrCreate();
    
        //1、解析key优先级
        Map<String, Integer> keyPriority = KeyPriorityUtil
                .getKeyPriority(config.getOptional(OneIdOptions.ONEID_KEY_LIST).get());
    }
}
