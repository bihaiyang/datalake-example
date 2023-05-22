package org.bii.example.iceberg.flink.sql.catalog;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/22
 */
public class InspectSQL {
    
    public static String history_sql = "SELECT * FROM hadoop.datalake.`ods_test$history`;";
    
    public static String metadata_log_sql = "SELECT * from hadoop.datalake.`ods_test$metadata_log_entries`;";
    
    public static String snapshots_list_sql = "SELECT * FROM hadoop.datalake.`ods_test$snapshots`;";
    
    
}
