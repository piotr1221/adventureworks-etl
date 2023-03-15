//import datacleaner.DataCleanerOrchestrator
import org.apache.spark.sql.SparkSession
import sources.postgresql.{PostgresTableExtractor, PostgresTableMetadata}
import adventureworks.layers.workload.domain.dimension._
import adventureworks.layers.workload.domain.fact._
import datacleaner.DataCleanerOrchestrator
import sources.DataMetadata
import sources.parquet.ParquetDataMetadata

object Main {  
  def main(args: Array[String]): Unit = {
    val adventureWorksPostgresSource = getAdventureWorksPostgresSource
    val adventureWorksPostgresTableMetadata = getAdventureWorksPostgresDataMetadata

    adventureWorksPostgresSource.extract(adventureWorksPostgresTableMetadata)

//    val dataCleanerOrchestrator = new DataCleanerOrchestrator()
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("AdventureWorks ETL")
      .getOrCreate()
  }

  def getAdventureWorksPostgresSource = new PostgresTableExtractor(
    createSparkSession(),
    "jdbc",
    "org.postgresql.Driver",
    "jdbc:postgresql://localhost:5432/Adventureworks",
    "postgres",
    "postgres"
  )

  def getAdventureWorksPostgresDataMetadata: Array[DataMetadata] = Array[DataMetadata](
    PostgresTableMetadata("person", "address", Address.schema),
    PostgresTableMetadata("person", "countryregion", CountryRegion.schema),
    PostgresTableMetadata("sales", "customer", Customer.schema),
    PostgresTableMetadata("humanresources", "employee", Employee.schema),
    PostgresTableMetadata("person", "person", Person.schema),
    PostgresTableMetadata("production", "product", Product.schema),
    PostgresTableMetadata("production", "productcategory", ProductCategory.schema),
    PostgresTableMetadata("production", "productsubcategory", ProductSubcategory.schema),
    PostgresTableMetadata("sales", "salesperson", SalesPerson.schema),
    PostgresTableMetadata("sales", "salesterritory", SalesTerritory.schema),
    PostgresTableMetadata("sales", "specialoffer", SpecialOffer.schema),
    PostgresTableMetadata("sales", "specialofferproduct", SpecialOfferProduct.schema),
    PostgresTableMetadata("person", "stateprovince", StateProvince.schema),
    PostgresTableMetadata("sales", "store", Store.schema),
    PostgresTableMetadata("sales", "currencyrate", CurrencyRate.schema),
    PostgresTableMetadata("sales", "salesorderdetail", SalesOrderDetail.schema),
    PostgresTableMetadata("sales", "salesorderheader", SalesOrderHeader.schema),
    PostgresTableMetadata("sales", "salestaxrate", SalesTaxRate.schema),
  )

  def sourceMetadataToParquet(dataMetadataArray: Array[DataMetadata]): Array[ParquetDataMetadata] = {
    dataMetadataArray.map(_.toParquetSourceMetadata())
  }

}