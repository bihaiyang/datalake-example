package org.bii.example.iceberg.flink;


import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.bii.example.iceberg.flink.util.ExecuteSql;
import org.bii.example.iceberg.flink.util.SqlCommandParser;
import org.bii.example.iceberg.flink.util.SqlFileParser;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/15
 */
public class FlinkSqlIcebergReader {
    
    
    public static void main(String[] args) {
        Map<String, String> systemEnv = System.getenv();
        systemEnv.put("HADOOP_CONF_DIR",
                "/Users/bihaiyang/IdeaProjects/github-workspace/datalake-example/iceberg-example/flink-iceberg-example/src/main/resources/");
       
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
    
        StreamTableEnvironment tEnv = StreamTableEnvironment
                .create(env, settings);
         String sql =
                 "create catalog hadoop "
                 + "with (\n"
                 + "'type'='iceberg',\n"
                 + "'catalog-type'='hadoop',\n"
                 + "'catalog-name'='hadoop',\n"
                 + "'warehouse'='alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/iceberg/'\n"
                 + ");\n"
                 + "USE CATALOG hadoop;\n"
                 + "create database if not exists hadoop.datalake; \n"
                 

                 + "CREATE TABLE if not exists hadoop.datalake.ods_test( \n"
                 + "    id   BIGINT,\n"
                 + "    data STRING,\n"
                 + "    category string\n"
                 + ");\n"

    
                + "SELECT * FROM ods_test;";
        List<SqlCommandParser> sqlCommandCallList = SqlFileParser.fileToSql(sql);
        ExecuteSql.exeSql(sqlCommandCallList, tEnv);
        
    
    }
}
