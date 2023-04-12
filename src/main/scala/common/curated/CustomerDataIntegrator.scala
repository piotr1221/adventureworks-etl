package common.curated

import common.DataIntegrator
import org.apache.spark.sql.SparkSession
import sources.parquet.ParquetDataUnit

class CustomerDataIntegrator extends DataIntegrator {
  override def integrate(spark: SparkSession): Unit = {
    val project = "adventureworks"

    parquetSource.read(spark, ParquetDataUnit.landing(project, "customer"))
      .createOrReplaceTempView("customer")
    parquetSource.read(spark, ParquetDataUnit.landing(project, "person"))
      .createOrReplaceTempView("person")
    parquetSource.read(spark, ParquetDataUnit.landing(project, "salesterritory"))
      .createOrReplaceTempView("salesterritory")

    val denormalizedCustomerQuery =
      """
        | SELECT
        |   cus.customerid,
        |   per.persontype,
        |   per.emailpromotion,
        |   terr.name AS countryname,
        |   terr.countryregioncode,
        |   terr.group AS countrygroup
        | FROM
        |   customer AS cus
        | LEFT JOIN
        |   person AS per
        | ON
        |   cus.personid = per.businessentityid
        | LEFT JOIN
        |   salesterritory AS terr
        | ON
        |   cus.territoryid = terr.territoryid
        | ORDER BY
        |   cus.customerid ASC
        |""".stripMargin

    val denormalizedCustomerDf = spark.sql(denormalizedCustomerQuery)
    parquetSource.writeParquet(denormalizedCustomerDf, ParquetDataUnit.curated("customer"))
  }
}
