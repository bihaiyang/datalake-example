package org.bii.example.iceberg.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * @author bihaiyang
 * @since 2022/11/28
 * @desc
 */
object IcebergApp {

  lazy val log: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkOperateIceberg")
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
      .config("spark.sql.catalog.hadoop.warehouse", "alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/")
      .getOrCreate()

    /***
     * iceberg spark 表创建与传统的 spark 表创建方式有所不同，
     * 基于 iceberg 格式创建 hadoop catalog的模型存储与 hadoop warehouse中，
     *  表相关属性：https://iceberg.apache.org/docs/latest/configuration/
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
        | (1,"zs","iceberg"),
        | (2,"ls","iceberg"),
        | (3,"ww","iceberg")
      """.stripMargin)

    //2.sql 方式读取Iceberg中的数据
    spark.sql(
      """
        |select * from hadoop.datalake.ods_test
      """.stripMargin).show()

    val df: DataFrame = spark.table("hadoop.datalake.ods_test")
    df.show()


    //3、查询快照表
    spark.sql(
      """
        |select * from hadoop.datalake.ods_test.snapshots
        |""".stripMargin).show(false)

    //4、查询历史数据
    spark.sql(
      """
        |select * from hadoop.datalake.ods_test.history
        |""".stripMargin).show(false)

    //5、查询所有历史数据文件
    spark.sql(
      """
        |select * from hadoop.datalake.ods_test.files
        |""".stripMargin)

    //6、查询所有的manifests 信息文件
    spark.sql(
      """
        |select * from hadoop.datalake.ods_test.manifests
        |""".stripMargin).show(false)


    val iceDf: DataFrame = spark.read.format("iceberg").load("alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/datalake/ods_test")
    iceDf.show()



  }
}
