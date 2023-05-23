package org.bii.example.iceberg.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * @author bihaiyang
 * @since 2023/05/23
 * @desc
 */
object IcebergWriteApp {

  lazy val log: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("IcebergDdlApp")
      .config("fs.alluxio.impl", "alluxio.hadoop.FileSystem")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop.type", "hadoop")
      .config("spark.sql.catalog.hadoop.warehouse", "alluxio://alluxio-master-test-0.default.svc.cluster.local:19998/iceberg/")
      .getOrCreate()


    //1、创建 iceberg 表
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS hadoop.datalake.ods_test (
        |    id bigint,
        |    data string,
        |    category string)
        |USING iceberg
        |with (
        |'format-version'='2',
        |'write.upsert.enabled'='true')
      """.stripMargin
    )

    //1、创建 iceberg 表
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS hadoop.datalake.ods_test_di (
        |    id bigint,
        |    data string,
        |    category string)
        |USING iceberg
        |with (
        |'format-version'='2',
        |'write.upsert.enabled'='true')
      """.stripMargin
    )

    spark.sql(
      """
        |insert into hadoop.datalake.ods_test_di values
        | (1,"zs","我不喜欢日本"),
        | (2,"ls","这是一个伸手不见五指的黑夜"),
        | (3,"ww","this is apple")
      """.stripMargin).show()


    /**
     * t2 表不能有重复字段
     * 原子性, 不支持并发, 单条sql 执行会持有catalog 锁
     */
    spark.sql(
      """
        |MERGE INTO hadoop.datalake.ods_test tt1 USING ( SELECT * FROM hadoop.datalake.ods_test_di ) tt2
        |ON ( tt1.id = tt2.id )
        |WHEN MATCHED THEN
        |UPDATE
        |	SET tt1.id = tt2.id, tt1.data = tt2.data, tt1.category = tt2.category
        |	WHEN NOT MATCHED THEN
        |	INSERT (id, data,category)
        |VALUES
        |	(tt2.id, tt2.data, tt2.category)
      """.stripMargin).show()


 /*   spark.sql(
      """
        |MERGE INTO hadoop.datalake.ods_test tt1 USING ( SELECT * FROM hadoop.datalake.ods_test ) tt2
        |ON ( tt1.id = tt2.id )
        |WHEN MATCHED THEN
        |UPDATE
        |	SET tt1.id = tt2.id, tt1.data = tt2.data, tt1.category = tt2.category
        |	WHEN NOT MATCHED THEN
        |	INSERT (id, data,category)
        |VALUES
        |	(tt2.id, tt2.data, tt2.category)
      """.stripMargin).show()

*/

  }
}
