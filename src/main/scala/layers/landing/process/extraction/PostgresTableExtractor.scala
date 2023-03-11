package layers.landing.process.extraction

import org.apache.spark.sql.{SparkSession, Encoder, Encoders, Dataset, Row}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode

import layers.landing.domain.dimension._
import layers.landing.domain.fact._

class PostgresTableExtractor(
  spark: SparkSession,
  format: String,
  driver: String,
  url: String,
  user: String,
  password: String
) extends TableExtractor(spark) {
  val tablesMetadata = loadMetadata
  import spark.implicits._

  private def loadMetadata: Array[TableMetadata] = {
    Array(
      TableMetadata("person", "address", Address.schema),
      TableMetadata("person", "countryregion", CountryRegion.schema),
      TableMetadata("sales", "customer", Customer.schema),
      TableMetadata("humanresources", "employee", Employee.schema),
      TableMetadata("person", "person", Person.schema),
      TableMetadata("production", "product", Product.schema),
      TableMetadata("production", "productcategory", ProductCategory.schema),
      TableMetadata("production", "productsubcategory", ProductSubcategory.schema),
      TableMetadata("sales", "salesperson", SalesPerson.schema),
      TableMetadata("sales", "salesterritory", SalesTerritory.schema),
      TableMetadata("sales", "specialoffer", SpecialOffer.schema),
      TableMetadata("sales", "specialofferproduct", SpecialOfferProduct.schema),
      TableMetadata("person", "stateprovince", StateProvince.schema),
      TableMetadata("sales", "store", Store.schema),
      TableMetadata("sales", "currencyrate", CurrencyRate.schema),
      TableMetadata("sales", "salesorderdetail", SalesOrderDetail.schema),
      TableMetadata("sales", "salesorderheader", SalesOrderHeader.schema),
      TableMetadata("sales", "salestaxrate", SalesTaxRate.schema),
    )
  }

  def startExtraction(): Unit = {
    for (tableMetadata <- tablesMetadata) {
      // println("/\\"*50)
      println(tableMetadata.table)
      val df = spark.read
        .format(format)
        .option("driver", driver)
        .option("url", url)
        .option("dbtable", tableMetadata.qualifiedName)
        .option("user", user)
        .option("password", password)
        .schema(tableMetadata.dataFrameSchema)
        .load()

      persistTableInDatalake(df, tableMetadata.table)
      // println("/\\"*50)
    }  
  }

  def persistTableInDatalake[U](df: Dataset[U], table: String): Unit = {
    df.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(concatTableNameToLandingPathAsParquet(table))
  }

}

