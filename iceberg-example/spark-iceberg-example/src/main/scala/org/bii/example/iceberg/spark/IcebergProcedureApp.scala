package org.bii.example.iceberg.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * @author bihaiyang
 * @since 2023/05/15
 * @desc
 */
object IcebergProcedureApp {

  lazy val log: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("IcebergQueryApp")
      .config("fs.alluxio.impl", "alluxio.hadoop.FileSystem")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      //.config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      //指定hive catalog, catalog名称为hive_prod
      /*     .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
           .config("spark.sql.catalog.hive_catalog.type", "hive")
           .config("spark.sql.catalog.hive_catalog.uri", "thrift://metastore-host:port")*/
      //.config("iceberg.engine.hive.enabled", "true")

      //指定hadoop catalog，catalog名称为hadoop_prod
      .config("spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop.type", "hadoop")
      .config("spark.sql.catalog.hadoop.warehouse", "alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/iceberg/")
      .getOrCreate()


    /**
     * 6、使用快照创建临时表有两种方式
     * - 使用spark api的形式创建
     * - 使用 sql 方式创建
     * -- 使用sql 创建有两种方式
     *    -- 使用默认存储路径存放快照表
     *    CALL hadoop.system.snapshot('datalake.ods_test', 'datalake.ods_test_snap')
     *    -- 指定位置存放
     *    CALL hadoop.system.snapshot('datalake.ods_test', 'datalake.ods_test_snap',"alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/datalake/ods_test_snap")
     *
     *    快照表直接删除即可
     */
    /*
        spark.read
          .option("snapshot-id", 4470827100986049465L)
          .format("iceberg")
          .load("alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/datalake/ods_test")
          .show()
    */

  /*  spark.sql(
      """
        | CREATE DATABASE IF NOT EXISTS hadoop.datalake
        | COMMENT 'iceberg测试库'
        | LOCATION 'alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/datalake'
        | WITH DBPROPERTIES (catalog_type = 'iceberg')
        |""".stripMargin)*/

/*    spark.sql(
      """
        |call hadoop.system.set_current_snapshot('datalake.ods_test',4470827100986049465)
        |""".stripMargin)*/


   /* spark.sql(
      """
        |CALL hadoop.system.cherrypick_snapshot(table => 'datalake.ods_test', snapshot_id => 4872069699094079285)
        |""".stripMargin)
      .show()*/

    //合并小文件
    spark.sql(
      """
        |CALL hadoop.system.rewrite_data_files('datalake.ods_test')
        |""".stripMargin)
      .show()

    //合并表清单
    spark.sql(
      """
        |CALL hadoop.system.rewrite_manifests('datalake.ods_test')
        |""".stripMargin)
      .show()


    spark.sql(
      """
        |CALL hadoop.system.expire_snapshots(table => 'datalake.ods_test')
        |""".stripMargin)

    spark.sql(
      """
        |select * from hadoop.datalake.ods_test.snapshots
        |""".stripMargin).show(false)


  }

}
