package org.bii.example.iceberg.flink;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.bii.example.iceberg.flink.conf.FlinkSqlConf;
import org.bii.example.iceberg.flink.sql.CreateCatalogSQL;
import org.bii.example.iceberg.flink.sql.InspectSQL;
import org.bii.example.iceberg.flink.sql.QuerySQL;
import org.bii.example.iceberg.flink.util.ExecuteSql;
import org.bii.example.iceberg.flink.util.SqlCommandParser;
import org.bii.example.iceberg.flink.util.SqlFileParser;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/23
 */
public class FlinkSqlIcebergQuery {
    
    
    public static void main(String[] args) {
    
        StreamTableEnvironment env = FlinkSqlConf.getEnv();
        List<String> sqlList = new ArrayList();
        CreateCatalogSQL.init(sqlList);
        sqlList.add(QuerySQL.QUERY_SQL);
    
        List<SqlCommandParser> sqlCommandCallList = SqlFileParser.sqlListParse(sqlList);
        TableResult tableResult = ExecuteSql.exeSql(sqlCommandCallList, env);
        tableResult.print();
    }
}
