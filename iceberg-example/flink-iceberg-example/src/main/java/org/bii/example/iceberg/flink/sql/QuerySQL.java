package org.bii.example.iceberg.flink.sql;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/22
 */
public class QuerySQL {
    
    /**
     *@see https://iceberg.apache.org/docs/latest/flink-queries/#flink-streaming-read
     *  流式从当前快照读取所有数据，然后增量读取数据
     */
    private static String QUERY_SQL = " SELECT * FROM sample /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ;";
    
    /**
     * 读取start-snapshot-id 开始的所有增量快照数据，但不包括此快照
     */
    private static String QUERY_FROM_SNAP_SQL =
            "SELECT * FROM sample /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'start-snapshot-id'='3821550127947089987')*/ ;";
    
    /**
     * @see https://iceberg.apache.org/docs/latest/flink-queries/#reading-branches-and-tags-with-sql
     * 从branch b1 读取数据
     */
    private static String QUERY_FROM_BRANCH_SQL = "SELECT * FROM table /*+ OPTIONS('branch'='b1') */ ;";
    
    /**
     * 读取标签t1数据
     */
    private static String QUERY_FROM_TAG_SQL = "SELECT * FROM table /*+ OPTIONS('tag'='t1') */;";
    
    /**
     * 流式读取标签数据
     */
    private static String QUERY_FROM_SCAN_TAG_SQL = "SELECT * FROM table /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'start-tag'='t1', 'end-tag'='t2') */;";
    

    
    
}
