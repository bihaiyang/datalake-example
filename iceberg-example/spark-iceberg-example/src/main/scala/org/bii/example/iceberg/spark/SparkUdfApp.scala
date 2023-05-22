package org.bii.example.iceberg.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * @author bihaiyang
 * @since 2023/05/22
 * @desc
 */
object SparkUdfApp {

  lazy val log: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkUdfApp")
      .config("fs.alluxio.impl", "alluxio.hadoop.FileSystem")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop.type", "hadoop")
      .config("spark.sql.catalog.hadoop.warehouse", "alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/iceberg/")
      .getOrCreate()


    //1.注册udf
    spark.sql(
      """
        |CREATE FUNCTION to_jieba_array AS 'io.terminus.horus.udf.JiebaWordUdfSegmenter';
        |
        |""".stripMargin).show()

    spark.sql(
      """
        |desc function to_jieba_array
        |""".stripMargin).show(false)

    spark.sql(
      """
        |select * from hadoop.datalake.ods_test
        |""".stripMargin).show()

    spark.sql(
      """
        |select to_jieba_array(category)  from hadoop.datalake.ods_test
        |""".stripMargin).show(false)


  }
}