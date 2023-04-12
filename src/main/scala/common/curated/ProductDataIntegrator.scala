package common.curated

import common.DataIntegrator
import org.apache.spark.sql.SparkSession
import sources.parquet.ParquetDataUnit

class ProductDataIntegrator extends DataIntegrator {
  override def integrate(spark: SparkSession): Unit = {
    val project = "adventureworks"

    parquetSource.read(spark, ParquetDataUnit.landing(project, "product"))
      .createOrReplaceTempView("product")
    parquetSource.read(spark, ParquetDataUnit.landing(project, "productcategory"))
      .createOrReplaceTempView("productcategory")
    parquetSource.read(spark, ParquetDataUnit.landing(project, "productsubcategory"))
      .createOrReplaceTempView("productsubcategory")

    val productColumns = Vector("productid", "name", "productnumber", "makeflag", "finishedgoodsflag", "color",
      "safetystocklevel", "reorderpoint", "standardcost", "listprice", "size", "sizeunitmeasurecode", "weight",
      "weightunitmeasurecode", "daystomanufacture", "productline", "class", "style", "sellstartdate", "sellenddate",
      "discontinueddate").map(column => s"p.${column}").mkString(",\n" + (" " * 3))

    // add productmodel

    val denormalizedProductQuery =
      s"""
        | SELECT
        |   ${productColumns},
        |   pc.name AS category,
        |   psc.name AS subcategory
        | FROM
        |   productcategory AS pc
        | LEFT JOIN
        |   productsubcategory AS psc
        | ON
        |   pc.productcategoryid = psc.productcategoryid
        | LEFT JOIN
        |   product AS p
        | ON
        |   psc.productsubcategoryid = p.productsubcategoryid
        | ORDER BY
        |   p.productid ASC
        |""".stripMargin

    val denormalizedProductDf = spark.sql(denormalizedProductQuery)
    parquetSource.writeParquet(denormalizedProductDf, ParquetDataUnit.curated("product"))
  }
}
