package org.bii.example.iceberg.flink;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.bii.example.iceberg.flink.conf.FlinkSqlConf;
import org.bii.example.iceberg.flink.sql.CreateCatalogSQL;
import org.bii.example.iceberg.flink.sql.InspectSQL;
import org.bii.example.iceberg.flink.util.ExecuteSql;
import org.bii.example.iceberg.flink.util.SqlCommandParser;
import org.bii.example.iceberg.flink.util.SqlFileParser;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/22
 */
public class FlinkSqlIceInspectingTables {
    

    
    public static void main(String[] args) {
    
        StreamTableEnvironment env = FlinkSqlConf.getEnv();
        List<String> sqlList = new ArrayList();
        CreateCatalogSQL.init(sqlList);
        sqlList.add(InspectSQL.HISTORY_LIST);
        sqlList.add(InspectSQL.SNAPSHOTS_LIST_SQL);
        sqlList.add(InspectSQL.METADATA_LOG);
        sqlList.add(InspectSQL.SNAPSHOT_JOIN_HISTORY);
        sqlList.add(InspectSQL.MANIFESTS_SQL);
    
        List<SqlCommandParser> sqlCommandCallList = SqlFileParser.sqlListParse(sqlList);
        TableResult tableResult = ExecuteSql.exeSql(sqlCommandCallList, env);
        tableResult.print();
    }
}
