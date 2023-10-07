package com.bii.oneid.consts;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/09/25
 */
public class OneIdConsts {

        /**
         * 维度表
         */
        public static final String DIM = "dim";

        /**
         * 事实表
         */
        public static final String DWD = "dwd";

        /**
         * union all
         */
        public static final String UNION_ALL = " UNION ALL ";

        public static class IdTypeRelationshipPairConst {

                /**
                 * oneid_relation_config oneid 关系对表 简表脚本
                 */
                public static final String RELATION_CONFIG_DDL_SCRIPT = " CREATE TABLE IF NOT EXISTS ${database}.oneid_relation_config"
                                + " ("
                                + "    left_key_type  STRING,"
                                + "    left_key_id    STRING,"
                                + "    right_key_type STRING,"
                                + "    right_key_id   STRING,"
                                + "    init_weight    DOUBLE"
                                + " ) PARTITIONED BY (ds STRING, keytype STRING, source_table STRING)"
                                + "   ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
                                + "        WITH SERDEPROPERTIES ("
                                + "        'serialization.format' = '1'"
                                + "        )"
                                + "    STORED AS"
                                + "        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'"
                                + "        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
                                + "    TBLPROPERTIES ("
                                + "        'transient_lastDdlTime' = '1632731291'"
                                + " );";

                /**
                 * oneid 无ID关系对处理
                 */
                public static final String SINGLE_CONFIG_DDL_SCRIPT = " CREATE TABLE IF NOT EXISTS ${database}.oneid_single_config"
                                + " ( "
                                + "    key_id      STRING,"
                                + "    key_type     STRING,"
                                + "    init_weight DOUBLE"
                                + " ) PARTITIONED BY (ds STRING, source_table STRING)"
                                + "    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
                                + "        WITH SERDEPROPERTIES ("
                                + "        'serialization.format' = '1'"
                                + "        )"
                                + "    STORED AS"
                                + "        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'"
                                + "        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
                                + "    TBLPROPERTIES ("
                                + "        'transient_lastDdlTime' = '1632731291'"
                                + " );";

                /**
                 * dwd 事实表关系对处理子sql
                 */
                public static final String SINGLE_RELATION_WRITE_SCRIPT = " INSERT OVERWRITE TABLE ${database}.oneid_single_config "
                                + " PARTITION (ds = '${biz_date}', source_table = '${src_table}') "
                                + " SELECT DISTINCT  ${key_id}  AS key_id, "
                                + "                '${key_type}' AS key_type, "
                                + "                 ${init_weight} AS init_weight "
                                + " FROM ${database}.${src_table} "
                                + " WHERE coalesce(cast(${key_id} AS STRING), '') != ''; ";

                /**
                 * dim 维表表关系对处理sql 只有一个ID的表处理
                 */
                public static final String DIM_MODEL_RELATION_DEAL_SCRIPT = " INSERT OVERWRITE TABLE ${database}.oneid_relation_config "
                                + " PARTITION (ds = '${biz_date}', key_type = 'dim', source_table = '${src_table}') "
                                + " SELECT left_key_type"
                                + "     , left_key_id"
                                + "     , right_key_type"
                                + "     , right_key_id"
                                + "     , init_weight"
                                + " FROM ( %s ) tt0;";

                /**
                 * dim 维表表关系对处理子sql
                 */
                public static final String DIM_MODEL_RELATION_SUB_SCRIPT = " SELECT DISTINCT '${left_key_type}' AS left_key_type"
                                + "              ,  ${left_key_id}   AS left_key_id"
                                + "              , '${right_key_type}' AS right_key_type"
                                + "              ,  ${right_key_id}   AS right_key_id"
                                + "              ,  ${init_weight}   AS init_weight"
                                + " FROM ${src_table}"
                                + " WHERE NOT (coalesce(CAST(${left_key_id} AS STRING)"
                                + "               , '') = ''"
                                + "    AND coalesce(CAST(${right_key_id} AS STRING)"
                                + "            , '') = ''"
                                + "    )";

                /**
                 * dwd 事实表关系对处理sql
                 */
                public static final String DWD_MODEL_RELATION_DEAL_SCRIPT = "INSERT OVERWRITE TABLE ${database}.oneid_relation_config "
                                + " PARTITION (ds = '${biz_date}', key_type = 'fct', source_table = '${src_table}')"
                                + " SELECT left_key_type"
                                + "     , left_key_id"
                                + "     , right_key_type"
                                + "     , right_key_id"
                                + "     , init_weight"
                                + " FROM ( %s ) tt0;";

                /**
                 * dwd 事实表关系对处理子sql
                 */
                public static final String DWD_MODEL_RELATION_SUB_SCRIPT = "         SELECT '${left_key_type}' AS left_key_type"
                                + "              , ${left_key_id}    AS left_key_id"
                                + "              , '${right_key_type}' AS right_key_type"
                                + "              , ${right_key_id}    AS right_key_id"
                                + "              , ${init_weight}    AS init_weight"
                                + "         FROM ${src_table}"
                                + "         WHERE NOT (coalesce(CAST(${left_key_id} AS STRING) "
                                + "                        , '') = ''"
                                + "             AND coalesce(CAST(${right_key_id} AS STRING) "
                                + "                     , '') = ''"
                                + "             )";

                /**
                 * 集合 oneid_relation_config left_key and right_key 以及 oneid_single_config 表中的
                 * key
                 */
                public static final String ONEID_ALL_KEY_ID = "CREATE TABLE IF NOT EXISTS ${namespace}.oneid_allkey_sd"
                                + "("
                                + "    key_id  STRING,"
                                + "    key_type STRING,"
                                + "    cnt     BIGINT"
                                + ") PARTITIONED BY (ds STRING)"
                                + "    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' "
                                + "        WITH SERDEPROPERTIES ("
                                + "        'serialization.format' = '1'"
                                + "        )"
                                + "    STORED AS"
                                + "        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' "
                                + "        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' "
                                + "    TBLPROPERTIES ("
                                + "        'transient_lastDdlTime' = '1632731291'"
                                + "        );"
                                + "INSERT OVERWRITE TABLE ${database}.oneid_allkey_sd PARTITION (ds = '${biz_date}') "
                                + " SELECT tmp.key_id "
                                + "     , tmp.key_type "
                                + "     , sum(tmp.cnt) AS cnt "
                                + " FROM (SELECT tmp.left_key_id   AS key_id "
                                + "           , count(1)          AS cnt "
                                + "           , tmp.left_key_type AS key_type "
                                + "      FROM (SELECT left_key_type "
                                + "                 , left_key_id "
                                + "                 , right_key_type "
                                + "                 , right_key_id "
                                + "                 , init_weight "
                                + "            FROM ${database}.oneid_relation_config "
                                + "            WHERE ds = '${biz_date}') tmp "
                                + "      GROUP BY tmp.left_key_type "
                                + "             , tmp.left_key_id "
                                + "      UNION ALL "
                                + "      SELECT tmp.right_key_id   AS key_id "
                                + "           , count(1)           AS cnt "
                                + "           , tmp.right_key_type AS key_type "
                                + "      FROM (SELECT left_key_type "
                                + "                 , left_key_id "
                                + "                 , right_key_type "
                                + "                 , right_key_id "
                                + "                 , init_weight "
                                + "            FROM ${database}.oneid_relation_config "
                                + "            WHERE ds = '${biz_date}') tmp "
                                + "      GROUP BY tmp.right_key_type "
                                + "             , tmp.right_key_id "
                                + "      UNION ALL "
                                + "      SELECT key_id "
                                + "           , count(1) AS cnt "
                                + "           , key_type "
                                + "      FROM ${database}.oneid_single_config "
                                + "      WHERE ds = '${biz_date}' "
                                + "      GROUP BY key_type, key_id "
                                + "     ) tmp "
                                + "WHERE keyid IS NOT NULL "
                                + "  AND length(keyid) > 0 "
                                + "GROUP BY key_id , key_type;";

        }

        public static class RelationshipWeight {

                public static String ONEID_RELATION_D_DDL_SCRIPT = "CREATE TABLE IF NOT EXISTS ${database}.oneid_relation_d"
                                + "("
                                + "    left_key_type  STRING COMMENT 'left id type',"
                                + "    left_key_id    STRING COMMENT 'id value',"
                                + "    right_key_type STRING COMMENT 'right id type',"
                                + "    right_key_id   STRING COMMENT 'right id type',"
                                + "    occ_time       DOUBLE COMMENT 'initial weight'"
                                + ") PARTITIONED BY (ds STRING)"
                                + "    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
                                + "        WITH SERDEPROPERTIES ("
                                + "        'serialization.format' = '1'"
                                + "        )"
                                + "    STORED AS"
                                + "        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'"
                                + "        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
                                + "    TBLPROPERTIES ("
                                + "        'transient_lastDdlTime' = '1632731291'"
                                + "        )"
                                + ";";

                public static final String ONEID_RELATION_SD_DDL_SCRIPT = " CREATE TABLE IF NOT EXISTS ${namespace}.oneid_relation_sd "
                                + "( "
                                + "    left_key_type  STRING, "
                                + "    left_key_id    STRING, "
                                + "    right_key_type STRING, "
                                + "    right_key_id   STRING, "
                                + "    cosine_score   DOUBLE, "
                                + "    pair_type STRING "
                                + ") "
                                + "    PARTITIONED BY (ds STRING COMMENT 'date') "
                                + "    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' "
                                + "        WITH SERDEPROPERTIES ( "
                                + "        'serialization.format' = '1' "
                                + "        ) "
                                + "    STORED AS "
                                + "        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' "
                                + "        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' "
                                + "    TBLPROPERTIES ( "
                                + "        'transient_lastDdlTime' = '1632731291' "
                                + "        ); ";

                /**
                 * 事实表关系对处理sql
                 */
                public static final String ONEID_DWD_RELATION_DEAL_SCRIPT = " INSERT OVERWRITE TABLE ${database}.oneid_relation_d PARTITION (ds = '${biz_date}')"
                                + " SELECT left_key_type"
                                + "     , left_key_id"
                                + "     , right_key_type"
                                + "     , right_key_id"
                                + "     , count(1) AS occ_time"
                                + " FROM ${database}.oneid_relation_config"
                                + " WHERE ds = '${biz_date}'"
                                + "  AND keytype = 'fct'"
                                + "  AND left_key_id IS NOT NULL"
                                + "  AND right_key_id IS NOT NULL"
                                + " GROUP BY left_key_type"
                                + "       , left_key_id"
                                + "       , right_key_type"
                                + "       , right_key_id;";

                public static final String ONEID_DIM_RELATION_DEAL_SCRIPT = " INSERT OVERWRITE TABLE ${database}.oneid_relation_sd PARTITION (ds = '${biz_date}')"
                                + " SELECT DISTINCT coalesce(dim.left_key_type, fct.left_key_type, '')   AS left_key_type"
                                + "              , coalesce(dim.left_key_id, fct.left_key_id, '')       AS left_key_id"
                                + "              , coalesce(dim.right_key_type, fct.right_key_type, '') AS right_key_type"
                                + "              , coalesce(dim.right_key_id, fct.right_key_id, '')     AS right_key_id"
                                + "              , coalesce(dim.init_weight, fct.cosine_score, 0.0)     AS cosine_score"
                                + "              , coalesce(dim.pair_type, fct.pair_type, '')           AS pair_type"
                                + " FROM (SELECT left_key_type"
                                + "           , left_key_id"
                                + "           , right_key_type"
                                + "           , right_key_id"
                                + "           , init_weight"
                                + "           , concat(left_key_type, '-', right_key_type) AS pair_type"
                                + "      FROM ${database}.oneid_relation_config"
                                + "      WHERE ds = '${biz_date}'"
                                + "        AND keytype = 'dim'"
                                + "        AND left_key_type IS NOT NULL"
                                + "        AND right_key_type IS NOT NULL"
                                + "     ) dim"
                                + "         FULL OUTER JOIN"
                                + "     (SELECT di.left_key_type                                 AS left_key_type"
                                + "           , di.left_key_id                                   AS left_key_id"
                                + "           , di.right_key_type                                AS right_key_type"
                                + "           , di.right_key_id                                  AS right_key_id"
                                + "           , di.occ_time / sqrt(df.cnt * dr.cnt)              AS cosine_score"
                                + "           , concat(di.left_key_type, '-', di.right_key_type) AS pair_type"
                                + "      FROM (SELECT *"
                                + "            FROM ${database}.oneid_relation_d"
                                + "            WHERE ds = '${biz_date}') di"
                                + "               JOIN"
                                + "           (SELECT *"
                                + "            FROM ${database}.oneid_allkey_sd"
                                + "            WHERE ds = '${biz_date}') df"
                                + "           ON di.left_key_id = df.key_id"
                                + "               AND di.left_key_type = df.keytype"
                                + "               JOIN"
                                + "           (SELECT *"
                                + "            FROM ${database}.oneid_allkey_sd"
                                + "            WHERE ds = '${biz_date}') dr"
                                + "           ON di.right_key_id = dr.key_id"
                                + "               AND di.right_key_type = dr.keytype"
                                + "     ) fct"
                                + "     ON dim.left_key_type = fct.left_key_type"
                                + "         AND dim.left_key_id = fct.left_key_id"
                                + "         AND dim.right_key_type = fct.right_key_type"
                                + "         AND dim.right_key_id = fct.right_key_id;";

        }

        public static class NoiseDataEliminate {
                /**
                 * 噪音数据消除语句, 将相同关系对数据大于 rule_value的关系对 结果数据写入graph的exec分区中
                 */
                public static final String NOISE_DATA_WRITE_TO_GRAPH_INPUT = " CREATE TABLE IF NOT EXISTS ${database}.oneid_graph_input_all"
                                + " ("
                                + "    id_pairs STRING"
                                + " ) PARTITIONED BY (ds STRING, key_type STRING)"
                                + "    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
                                + "        WITH SERDEPROPERTIES ("
                                + "        'serialization.format' = '1'"
                                + "        )"
                                + "    STORED AS"
                                + "        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'"
                                + "        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
                                + "    TBLPROPERTIES ("
                                + "        'transient_lastDdlTime' = '1632731291'"
                                + "        );"
                                + " INSERT OVERWRITE TABLE ${database}.oneid_graph_input_all "
                                + " PARTITION (ds = '${biz_date}', keytype = 'exc_key')"
                                + " SELECT key"
                                + " FROM (SELECT concat_ws(char(8), type, key) AS key"
                                + "      FROM (SELECT key"
                                + "                 , type"
                                + "                 , count(1) AS cnt"
                                + "            FROM (SELECT left_key_id   AS key"
                                + "                       , left_key_type AS type"
                                + "                  FROM (SELECT DISTINCT left_key_id"
                                + "                                      , left_key_type"
                                + "                                      , right_key_id"
                                + "                                      , right_key_type"
                                + "                        FROM ${database}.oneid_relation_sd"
                                + "                        WHERE ds = '${biz_date}') t"
                                + "                  UNION ALL"
                                + "                  SELECT right_key_id   AS key"
                                + "                       , right_key_type AS type"
                                + "                  FROM (SELECT DISTINCT left_key_id"
                                + "                                      , left_key_type"
                                + "                                      , right_key_id"
                                + "                                      , right_key_type"
                                + "                        FROM ${database}.oneid_relation_sd"
                                + "                        WHERE ds = '${biz_date}') t"
                                + "                 ) a"
                                + "            GROUP BY key"
                                + "                   , type) b"
                                + "      WHERE cnt >= ${rule_value} "
                                + "     ) c;";

        }

        public static class ErrRelationPairRemove {

                /**
                 * 同一 id 与另一个同类型的 id，关系对超 N 对，则 topN 以上均剪枝，一般是 100。
                 */
                public static final String ONEID_GRAPH_INPUT_ALL = "CREATE TABLE IF NOT EXISTS ${database}.oneid_graph_input_all "
                                + "( "
                                + "    id_pairs STRING "
                                + ") PARTITIONED BY (ds STRING, keyt_ype STRING) "
                                + "    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' "
                                + "        WITH SERDEPROPERTIES ( "
                                + "        'serialization.format' = '1' "
                                + "        ) "
                                + "    STORED AS "
                                + "        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' "
                                + "        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' "
                                + "    TBLPROPERTIES ( "
                                + "        'transient_lastDdlTime' = '1632731291' "
                                + "        ) "
                                + "; ";

                public static final String ONEID_RELATION_INPUT_SD = " CREATE TABLE IF NOT EXISTS ${database}.oneid_relation_input_sd"
                                + " ("
                                + "    `left_key_type`  STRING COMMENT '左ID类型',"
                                + "    `left_key_id`    STRING COMMENT '左ID',"
                                + "    `right_key_type` STRING COMMENT '右ID类型',"
                                + "    `right_key_id`   STRING COMMENT '右ID',"
                                + "    `cosine_score`   DOUBLE COMMENT '得分'"
                                + " )"
                                + "    COMMENT 'oneId输入表'"
                                + "    PARTITIONED BY (ds STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
                                + "    WITH SERDEPROPERTIES ("
                                + "        'serialization.format' = '1'"
                                + "        )"
                                + "    STORED AS"
                                + "        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'"
                                + "        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
                                + "    TBLPROPERTIES ("
                                + "        'transient_lastDdlTime' = '1632731291'"
                                + "        )"
                                + ";";

                public static final String ERR_REL_DEAL_SCRIPT_STEP1 =
                 " INSERT OVERWRITE TABLE ${database}.oneid_relation_input_sd PARTITION (ds = '${biz_date}')"
                                + " SELECT a.left_key_type"
                                + "     , a.left_key_id"
                                + "     , a.right_key_type"
                                + "     , a.right_key_id"
                                + "     , a.cosine_score"
                                + " FROM (SELECT left_key_id"
                                + "           , left_key_type"
                                + "           , right_key_id"
                                + "           , right_key_type"
                                + "           , cosine_score"
                                + "           , concat_ws(char(8), left_key_type, left_key_id)   AS left_key"
                                + "           , concat_ws(char(8), right_key_type, right_key_id) AS right_key"
                                + "      FROM ${database}.oneid_relation_sd"
                                + "      WHERE ds = '${bizdate}') a"
                                + "         LEFT OUTER JOIN"
                                + "     (SELECT id_pairs AS key"
                                + "           , ds"
                                + "      FROM ${database}.oneid_graph_input_all"
                                + "      WHERE ds = '${biz_date}'"
                                + "        AND keytype IN ('exc_key', 'exc_key_n')"
                                + "     ) b"
                                + "     ON a.left_key = b.key"
                                + "         LEFT OUTER JOIN"
                                + "     (SELECT id_pairs AS key"
                                + "           , ds"
                                + "      FROM ${database}.oneid_graph_input_all"
                                + "      WHERE ds = '${biz_date}'"
                                + "        AND keytype IN ('exc_key', 'exc_key_n')) d"
                                + "     ON a.right_key = d.key"
                                + " WHERE d.ds IS NULL"
                                + "  AND b.ds IS NULL;";

                public static final String ERR_REL_DEAL_SUB_SCRIPT_STEP2 = 
                " set spark.sql.hive.convertMetastoreParquet=false;"
                                + " INSERT OVERWRITE TABLE ${database}.oneid_relation_input_sd PARTITION (ds = '${biz_date}')"
                                + " SELECT left_key_type"
                                + "     , left_key_id"
                                + "     , right_key_type"
                                + "     , right_key_id"
                                + "     , cosine_score"
                                + " FROM (SELECT t.*"
                                + "           , row_number()"
                                + "            OVER (PARTITION BY left_key_id, left_key_type, right_key_type ORDER BY cosine_score DESC) AS num"
                                + "      FROM ${namespace}.oneid_relation_input_sd t"
                                + "      WHERE ds = '${bizdate}') a"
                                + " WHERE num <= ${rulevalue} "
                                + "  AND length(left_key_id) > 1"
                                + "  AND length(right_key_id) > 1;";

                public static final String ERR_REL_DEAL_SUB_SCRIPT_STEP3 = " set spark.sql.hive.convertMetastoreParquet=false;"
                                + " INSERT OVERWRITE TABLE ${namespace}.oneid_relation_input_sd PARTITION (ds = '${bizdate}')"
                                + " SELECT left_key_type"
                                + "     , left_key_id"
                                + "     , right_key_type"
                                + "     , right_key_id"
                                + "     , cosine_score"
                                + " FROM (SELECT t.*"
                                + "           , row_number()"
                                + "            OVER (PARTITION BY right_key_id, right_key_type, left_key_type ORDER BY cosine_score DESC) AS num"
                                + "      FROM ${namespace}.oneid_relation_input_sd t"
                                + "      WHERE ds = '${bizdate}') a"
                                + " WHERE num <= ${rulevalue}"
                                + "  AND length(left_key_id) > 1"
                                + "  AND length(right_key_id) > 1;";

                public static final String ONEID_OUTPUT_EXTEND_GRAPH = " CREATE TABLE IF NOT EXISTS ${database}.oneid_cc_output_graph"
                                + " ("
                                + "    `cluster_id` STRING COMMENT '簇ID',"
                                + "    `key_id`     STRING COMMENT 'key值',"
                                + "    `key_type`   STRING COMMENT 'key类型'"
                                + " )"
                                + "    COMMENT 'oneId图计算输出表'"
                                + "    PARTITIONED BY (ds STRING) "
                                + " ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
                                + "    WITH SERDEPROPERTIES ("
                                + "        'serialization.format' = '1'"
                                + "        )"
                                + "    STORED AS"
                                + "        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'"
                                + "        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
                                + "    TBLPROPERTIES ("
                                + "        'transient_lastDdlTime' = '1632731291'"
                                + "        )"
                                + ";"
                                + " ALTER TABLE ${namespace}.oneid_cc_output_graph"
                                + "    ADD IF NOT EXISTS PARTITION (ds = '${bizdate}');";

                public static final String ONEID_OUTPUT_EXTEND_SPLIT = "CREATE TABLE IF NOT EXISTS"
                                + " ${namespace}.oneid_cc_output_split"
                                + " ("
                                + "    `cluster_id` STRING COMMENT '簇ID',"
                                + "    `key_id`     STRING COMMENT 'key值',"
                                + "    `key_type`   STRING COMMENT 'key类型',"
                                + "    `score`      DOUBLE COMMENT '置信度'"
                                + " )"
                                + "    COMMENT 'oneId图计算输出表'"
                                + "    PARTITIONED BY (ds STRING,graph_type STRING) "
                                + "     ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
                                + "    WITH SERDEPROPERTIES ("
                                + "        'serialization.format' = '1'"
                                + "        )"
                                + "    STORED AS"
                                + "        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'"
                                + "        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
                                + "    TBLPROPERTIES ("
                                + "        'transient_lastDdlTime' = '1632731291'"
                                + "        )"
                                + " ;"
                                + " ALTER TABLE ${namespace}.oneid_cc_output_split"
                                + "    ADD IF NOT EXISTS PARTITION (ds = '${bizdate}',graph_type = 'graph');";

                public static final String ONEID_OUTPUT_EXTEND_SCRIPT = "CREATE TABLE IF NOT EXISTS ${database}.oneid_cc_output_extend"
                                + "("
                                + "    `one_id` STRING COMMENT '簇ID',"
                                + "    `key_id`     STRING COMMENT 'key值',"
                                + "    `key_type`   STRING COMMENT 'key类型',"
                                + "    `score`      DOUBLE COMMENT '置信度'"
                                + ")"
                                + "    COMMENT 'oneId图计算输出表'"
                                + "    PARTITIONED BY (ds STRING,graph_type STRING) "
                                + "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
                                + "    WITH SERDEPROPERTIES ("
                                + "        'serialization.format' = '1'"
                                + "        )"
                                + "    STORED AS"
                                + "        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'"
                                + "        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
                                + "    TBLPROPERTIES ("
                                + "        'transient_lastDdlTime' = '1632731291'"
                                + "        )"
                                + ";"
                                + "ALTER TABLE ${database}.oneid_cc_output_extend"
                                + "    ADD IF NOT EXISTS PARTITION (ds = '${biz_date}',graph_type = 'extend');"
                                + ""
                                + "ALTER TABLE ${database}.oneid_cc_output_extend"
                                + "    ADD IF NOT EXISTS PARTITION (ds = '${biz_date}',graph_type = 'single');";

        }
}
