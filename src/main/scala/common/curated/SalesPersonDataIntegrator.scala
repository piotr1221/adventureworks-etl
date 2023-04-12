package common.curated

import common.DataIntegrator
import org.apache.spark.sql.SparkSession
import sources.parquet.ParquetDataUnit

class SalesPersonDataIntegrator extends DataIntegrator {
  override def integrate(spark: SparkSession): Unit = {
    val project = "adventureworks"
    parquetSource.read(spark, ParquetDataUnit.landing(project, "employee"))
      .createOrReplaceTempView("employee")
    parquetSource.read(spark, ParquetDataUnit.landing(project, "salesperson"))
      .createOrReplaceTempView("salesperson")
    parquetSource.read(spark, ParquetDataUnit.landing(project, "salesterritory"))
      .createOrReplaceTempView("salesterritory")

    val denormalizedSalesPersonQuery =
      """
        | SELECT
        |   sp.businessentityid AS salespersonid,
        |   e.jobtitle,
        |   sp.salesquota,
        |   sp.bonus,
        |   sp.commissionpct,
        |   sp.salesytd,
        |   sp.saleslastyear,
        |   e.maritalstatus,
        |   e.gender,
        |   e.hiredate,
        |   e.salariedflag,
        |   e.vacationhours,
        |   e.sickleavehours,
        |   terr.name AS countryname,
        |   terr.countryregioncode,
        |   terr.group AS countrygroup
        | FROM
        |   salesperson AS sp
        | LEFT JOIN
        |   employee AS e
        | ON
        |   sp.businessentityid = e.businessentityid
        | LEFT JOIN
        |   salesterritory AS terr
        | ON
        |   sp.territoryid = terr.territoryid
        | ORDER BY
        |   sp.businessentityid ASC
        |""".stripMargin

    val denormalizedSalesPersonDf = spark.sql(denormalizedSalesPersonQuery)
    parquetSource.writeParquet(denormalizedSalesPersonDf, ParquetDataUnit.curated("salesperson"))
  }
}
