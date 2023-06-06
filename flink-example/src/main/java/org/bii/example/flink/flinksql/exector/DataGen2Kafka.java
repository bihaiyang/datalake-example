package org.bii.example.flink.flinksql.exector;

import java.util.List;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.bii.example.flink.conf.FlinkSqlConf;
import org.bii.example.flink.flinksql.util.ExecuteSql;
import org.bii.example.flink.flinksql.util.FileUtils;
import org.bii.example.flink.flinksql.util.SqlCommandParser;
import org.bii.example.flink.flinksql.util.SqlFileParser;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/06/05
 */
public class DataGen2Kafka {
    
    public static void main(String[] args) {
        
        StreamTableEnvironment env = FlinkSqlConf.getEnv();
        List<String> sql = FileUtils.readLineFromFile("/Users/bihaiyang/IdeaProjects/github-workspace/datalake-example/flink-example/src/main/flinksql/DataGen2Print.sql");
        List<SqlCommandParser> sqlCommandCallList = SqlFileParser.fileToSql(sql);
        TableResult tableResult = ExecuteSql.exeSql(sqlCommandCallList, env);
        tableResult.print();
    }
}
