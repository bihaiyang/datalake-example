package org.bii.example.iceberg.flink.sql.catalog;

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
                    + ");\n"
                    + "create database if not exists hadoop.datalake; \n"
                    + "CREATE TABLE if not exists hadoop.datalake.ods_test( \n"
                    + "    id   BIGINT,\n"
                    + "    data STRING,\n"
                    + "    category string\n"
                    + ");\n" ;
}
