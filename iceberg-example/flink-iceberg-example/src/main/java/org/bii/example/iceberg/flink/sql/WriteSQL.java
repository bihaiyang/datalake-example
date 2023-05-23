package org.bii.example.iceberg.flink.sql;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/22
 */
public class WriteSQL {
    
    
    /**
     * 支持 stream sql
     */
    public static String INSERT_SQL = "INSERT INTO hadoop.datalake.ods_test VALUES (1,'zs','我不喜欢日本');";
    
    
    //public static String INSERT_SELECT_SQL = "INSERT INTO hadoop.datalake.ods_test SELECT id, data from other_kafka_table;\n";
    /**
     * INSERT OVERWRITE 支持批式写入
     */
    public static String INSERT_OVER_SQL = "INSERT OVERWRITE hadoop.datalake.ods_test VALUES (1,'zs','我不喜欢日本');\n";
    
    /**
     * UPSERT
     */
    public static String UPSERT_SQL = "INSERT INTO hadoop.datalake.ods_test /*+ OPTIONS('upsert-enabled'='true') */ VALUES (1,'zs','我不喜欢日本');\n";
    
}
