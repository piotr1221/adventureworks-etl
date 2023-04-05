package common.curated

import common.DataIntegrator
import org.apache.spark.sql.SparkSession
import sources.parquet.ParquetDataUnit

class SalesFactDataIntegrator extends DataIntegrator {
  def integrate(spark: SparkSession): Unit = {
    parquetSource.read(spark, ParquetDataUnit.landing("adventureworks", "salesorderheader"))
      .createOrReplaceTempView("salesorderheader")
    parquetSource.read(spark, ParquetDataUnit.landing("adventureworks", "salesorderdetail"))
      .createOrReplaceTempView("salesorderdetail")

    val denormalizedSalesDf = spark.sql(
      s"""
        | SELECT
        |   salesorderheader.*,
        |   (
        |     SELECT
        |       COLLECT_LIST(struct(salesorderdetail.*))
        |     FROM
        |       salesorderdetail
        |     WHERE
        |       salesorderheader.salesorderid = salesorderdetail.salesorderid
        |   ) AS salesorderdetail
        | FROM
        |   salesorderheader
        | ORDER BY
        |   salesorderheader.salesorderid ASC
        |""".stripMargin
    )
    parquetSource.writeParquet(denormalizedSalesDf, ParquetDataUnit.curated("salesorders"))
  }
}