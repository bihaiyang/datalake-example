package org.bii.example.iceberg.flink.sql;

import java.util.List;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/05/22
 */
public class CreateCatalogSQL {
    
    /***
     * hadoop catalog
     */
    
    public static String CREATE_CATALOG_DB =
            "create catalog hadoop "
                    + "with (\n"
                    + "'type'='iceberg',\n"
                    + "'catalog-type'='hadoop',\n"
                    + "'catalog-name'='hadoop',\n"
                    + "'warehouse'='alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/iceberg/'\n"
                    + ");\n";
    
    
    public static String CREATE_DB = "create database if not exists hadoop.datalake;";
    
    
    public static String CREATE_TABLE = 
            "CREATE TABLE if not exists hadoop.datalake.ods_test( \n"
            + "    id   BIGINT,\n"
            + "    data STRING,\n"
            + "    category string\n"
            + ") with ('format-version'='2', 'write.upsert.enabled'='true');;\n";
    
    
    public static String CREATE_PRIMARY_TABLE =
            "CREATE TABLE if not exists hadoop.datalake.ods_test( \n"
                    + "    id   BIGINT,\n"
                    + "    data STRING,\n"
                    + "    category string\n"
                    + "PRIMARY KEY(`id`) NOT ENFORCED"
                    + ") with ('format-version'='2', 'write.upsert.enabled'='true');;\n";
    
    
    public static void init(List<String> list){
        list.add(CREATE_CATALOG_DB);
        list.add(CREATE_DB);
        list.add(CREATE_TABLE);
    }
}
