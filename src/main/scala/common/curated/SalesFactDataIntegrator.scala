package common.curated

import common.DataIntegrator
import org.apache.spark.sql.SparkSession
import sources.parquet.ParquetDataUnit
import org.apache.spark.sql.functions._

/**
Joins a sales order header with an array of sales order details and converts
 converts the currency of the monetary amounts to USD
*/
class SalesFactDataIntegrator extends DataIntegrator {
  def integrate(spark: SparkSession): Unit = {
    val project = "adventureworks"

    parquetSource.read(spark, ParquetDataUnit.landing(project, "salesorderheader"))
      .createOrReplaceTempView("salesorderheader")
    parquetSource.read(spark, ParquetDataUnit.landing(project, "salesorderdetail"))
      .createOrReplaceTempView("salesorderdetail")
    parquetSource.read(spark, ParquetDataUnit.landing(project, "currencyrate"))
      .createOrReplaceTempView("currencyrate")
    parquetSource.read(spark, ParquetDataUnit.landing(project, "store"))
      .createOrReplaceTempView("store")

    spark.udf.register("convertToDollars", (conversionRate: BigDecimal, monetaryAmount: BigDecimal) =>
      if (conversionRate == null) monetaryAmount else ( monetaryAmount / conversionRate )
    )

    val orderDetailColumns = Vector("salesorderdetailid", "carriertrackingnumber", "orderqty", "productid",
      "specialofferid", "unitpricediscount", "unitpriceindollars").mkString(",\n" + (" " * 9))

    val denormalizedSalesQuery = s"""
      | SELECT
      |   soheader.*,
      |   (
      |     SELECT
      |       COLLECT_LIST(struct(
      |         ${orderDetailColumns}
      |       ))
      |     FROM
      |       (
      |         SELECT
      |           *,
      |           convertToDollars(cr.averagerate, salesorderdetail.unitprice)
      |           AS unitpriceindollars
      |         FROM
      |           salesorderdetail
      |       ) AS sodetail
      |     WHERE
      |       soheader.salesorderid = sodetail.salesorderid
      |   ) AS salesorderdetail,
      |   convertToDollars(cr.averagerate, soheader.subtotal)
      |   AS subtotalindollars,
      |   convertToDollars(cr.averagerate, soheader.freight)
      |   AS freightindollars,
      |   convertToDollars(cr.averagerate, soheader.taxamt)
      |   AS taxamtindollars,
      |   convertToDollars(cr.averagerate, soheader.totaldue)
      |   AS totaldueindollars
      | FROM
      |   salesorderheader AS soheader
      | LEFT JOIN
      |   (
      |     SELECT
      |       currencyrateid,
      |       averagerate
      |     FROM
      |       currencyrate
      |   ) AS cr
      | ON
      |   soheader.currencyrateid = cr.currencyrateid
      | ORDER BY
      |   soheader.salesorderid ASC;
      |""".stripMargin

    val denormalizedSalesDf = spark.sql(denormalizedSalesQuery)
      .drop("currencyrateid", "totaldue", "rowguid","modifieddate", "subtotal", "freight", "taxamt")
    parquetSource.writeParquet(denormalizedSalesDf, ParquetDataUnit.curated("salesorders"))
  }
}