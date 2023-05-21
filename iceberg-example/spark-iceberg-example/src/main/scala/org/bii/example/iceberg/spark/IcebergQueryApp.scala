package org.bii.example.iceberg.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author bihaiyang
 * @since 2023/05/15
 * @desc
 */
object IcebergQueryApp{

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


    //1.sql 方式读取Iceberg中的数据
    spark.sql(
      """
        |select * from hadoop.datalake.ods_test
      """.stripMargin).show()

    spark.table("hadoop.datalake.ods_test").show()


    //2、查询快照表
    println("hadoop.datalake.ods_test.snapshots => ")
    spark.sql(
      """
        |select * from hadoop.datalake.ods_test.snapshots
        |""".stripMargin).show(false)

    //3、查询历史数据
    println("hadoop.datalake.ods_test.history => ")
    spark.sql(
      """
        |select * from hadoop.datalake.ods_test.history
        |""".stripMargin).show(false)

    println("hadoop.datalake.ods_test.files => ")
    //4、查询所有历史数据文件
    spark.sql(
      """
        |select * from hadoop.datalake.ods_test.files
        |""".stripMargin)

    //5、查询所有的manifests 信息文件
    spark.sql(
      """
        |select * from hadoop.datalake.ods_test.manifests
        |""".stripMargin).show(false)




  }


}
