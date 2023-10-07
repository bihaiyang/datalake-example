package com.bii.oneid.app;


import com.bii.oneid.compute.OneIdConnectedComponent;
import com.bii.oneid.configuration.Configuration;
import com.bii.oneid.configuration.OneIdOptions;
import com.bii.oneid.util.KeyPriorityUtil;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author bihaiyang
 * @since 2023/09/25
 */
public class OneIdGraphApp {
    
    private Logger log = LoggerFactory.getLogger(OneIdGraphApp.class);
    
    public OneIdGraphApp(Logger log) {
        this.log = log;
    }
    
    public static void main(String[] args) {
        
        Configuration config = new Configuration();
        SparkSession spark = SparkSession.builder()
                .appName("OneId graph component connect")
                .getOrCreate();
        
        //1、解析key优先级
        Map<String, Integer> keyPriority = KeyPriorityUtil
                .getKeyPriority(config.getOptional(OneIdOptions.ONEID_KEY_LIST).get());
    
        Dataset<Row> rowDataset = spark.read().parquet(
                config.getOptional(OneIdOptions.ONEID_LOCATION).get() + "/" +
                        config.getOptional(OneIdOptions.GRAPH_COMPONENT_INPUT_TABLE).get() + "/" +
                        config.getOptional(OneIdOptions.ONEID_PARTITION)).toDF(
                                "left_key_type", "left_key_id", "right_key_type", "right_key_id", "cosine_score");
        
        OneIdConnectedComponent oneIdConnectedComponent = new OneIdConnectedComponent();
        
        Dataset<Row> clusterDataSet = oneIdConnectedComponent.run(rowDataset, keyPriority);
        
        //中间结果数据存储
        clusterDataSet.coalesce(1).write().mode(SaveMode.Overwrite).parquet(
                config.getOptional(OneIdOptions.ONEID_LOCATION).get() + "/" +
                config.getOptional(OneIdOptions.GRAPH_COMPONENT_OUTPUT_TABLE).get() + "/" +
                config.getOptional(OneIdOptions.ONEID_PARTITION));
    }
}
