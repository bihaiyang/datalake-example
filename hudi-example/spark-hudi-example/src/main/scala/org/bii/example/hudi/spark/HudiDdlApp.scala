package org.bii.example.hudi.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * @author bihaiyang
 * @since 2023/05/24
 * @desc
 */
object HudiDdlApp {

  lazy val log: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("IcebergDdlApp")
      .config("fs.alluxio.impl", "alluxio.hadoop.FileSystem")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .getOrCreate()


    spark.sql(
      """
        |CREATE DATABASE datalake
        | location 'alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/datalake/hudi/'
        |
        |""".stripMargin).show(true)



    spark.sql(
      """
        |create table if not exists datalake.ods_test (
        |  id bigint,
        |  name string,
        |  ts bigint,
        |  dt string,
        |  hh string
        |)
        |using hudi
        |options(
        | `type` 'cow',
        | `primaryKey` 'id',
        |  path 'alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/datalake/hudi/ods_test'
        |)
        |partitioned by (dt, hh)
        |""".stripMargin).show(true)


    spark.sql(
      """
        |select * from datalake.ods_test
        |""".stripMargin).show(true)


    spark.sql(
      """
        |call show_commits(table => 'datalake.ods_test', limit => 10);
        |""".stripMargin).show(true)

    spark.sql(
      """
        |call show_commits_metadata(table => 'datalake.ods_test', limit => 10);
        |""".stripMargin).show(true)
  }
}
