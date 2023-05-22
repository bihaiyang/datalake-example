package org.bii.example.iceberg.flink;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.bii.example.iceberg.flink.conf.FlinkSqlConf;
import org.bii.example.iceberg.flink.sql.catalog.CreateCatalogSQL;
import org.bii.example.iceberg.flink.sql.catalog.InspectSQL;
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
        sqlList.add(CreateCatalogSQL.CREATE_CATALOG_DB);
        sqlList.add(InspectSQL.history_sql);
        sqlList.add(InspectSQL.snapshots_list_sql);
        sqlList.add(InspectSQL.metadata_log_sql);
    
        List<SqlCommandParser> sqlCommandCallList = SqlFileParser.sqlListParse(sqlList);
        ExecuteSql.exeSql(sqlCommandCallList, env);
    }
}
