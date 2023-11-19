package org.bii.example.flink.flinksql.exector;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.bii.example.flink.conf.FlinkSqlConf;
import org.bii.example.flink.flinksql.util.ExecuteSql;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/11/03
 */
public class TimeTest {
    
    public static void main(String[] args) {
    
        StreamTableEnvironment env = FlinkSqlConf.getEnv();
        
        TableResult tableResult = ExecuteSql.exeSQLFromFile(
                "/Users/bihaiyang/IdeaProjects/github-workspace/datalake-example/flink-example/src/main/flinksql/EventTime.sql",
                env);
        tableResult.print();
   
    
    }
}
