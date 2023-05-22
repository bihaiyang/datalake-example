package org.bii.example.iceberg.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


/**
 * @author bihaiyang
 * @since 2022/11/28
 * @desc
 */
object IcebergDdlApp {

  lazy val log: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("IcebergDdlApp")
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

    /** *
     * iceberg spark 表创建与传统的 spark 表创建方式有所不同，
     * 基于 iceberg 格式创建 hadoop catalog的模型存储与 hadoop warehouse中，
     * 表相关属性：https://iceberg.apache.org/docs/latest/configuration/
     */
    //1、创建 iceberg 表
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS hadoop.datalake.ods_test (
        |    id bigint,
        |    data string,
        |    category string)
        |USING iceberg
      """.stripMargin
    )

    spark.sql(
      """
        |insert into hadoop.datalake.ods_test values
        | (1,"zs","我不喜欢日本"),
        | (2,"ls","这是一个伸手不见五指的黑夜"),
        | (3,"ww","this is apple")
      """.stripMargin).show()

  }
}
