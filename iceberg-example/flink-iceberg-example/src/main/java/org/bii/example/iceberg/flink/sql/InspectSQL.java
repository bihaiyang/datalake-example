package org.bii.example.iceberg.flink.sql;

/**
 * @author bihaiyang
 * @desc https://iceberg.apache.org/docs/latest/flink-queries/#inspecting-tables
 * @since 2023/05/22
 */
public class InspectSQL {
    
    public static String HISTORY_LIST = "SELECT * FROM hadoop.datalake.`ods_test$history`;";
    
    public static String METADATA_LOG = "SELECT * from hadoop.datalake.`ods_test$metadata_log_entries`;";
    
    public static String SNAPSHOTS_LIST_SQL = "SELECT * FROM hadoop.datalake.`ods_test$snapshots`;";
    
    public static String SNAPSHOT_JOIN_HISTORY = "select\n"
            + "    h.made_current_at,\n"
            + "    s.operation,\n"
            + "    h.snapshot_id,\n"
            + "    h.is_current_ancestor,\n"
            + "    s.summary['flink.job-id']\n"
            + "from hadoop.datalake.`ods_test$history` h\n"
            + "join hadoop.datalake.`ods_test$snapshots` s\n"
            + "  on h.snapshot_id = s.snapshot_id\n"
           /* + "order by made_current_at"*/;
    
    public static String MANIFESTS_SQL = "SELECT * FROM hadoop.datalake.`ods_test$manifests`;";
}
