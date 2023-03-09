package layers.landing.process

import org.apache.spark.sql.{SparkSession, Encoder, Encoders, Dataset, Row}
import org.apache.spark.sql.types.StructType

import layers.landing.domain.dimension._


object TableExtractor {
  val tableExtractionMetadata = loadMetadata

  def loadMetadata: TableExtractionMetadata = {
    val tablesMetadata = Array(
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
      TableMetadata("sales", "store", Store.schema)
    )

    TableExtractionMetadata(
      "jdbc",
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost:5432/Adventureworks",
      "postgres",
      "postgres",
      tablesMetadata
    )
  }

  def start(spark: SparkSession): Unit = {
    import spark.implicits._

    for (tableMetadata <- tableExtractionMetadata.tables) {
      println("/\\"*50)
      println(tableMetadata.table)
      val df = spark.read
        .format(tableExtractionMetadata.format)
        .option("driver", tableExtractionMetadata.driver)
        .option("url", tableExtractionMetadata.url)
        .option("dbtable", tableMetadata.qualifiedName)
        .option("user", tableExtractionMetadata.user)
        .option("password", tableExtractionMetadata.password)
        .schema(tableMetadata.dataFrameSchema)
        .load()
        .show(1)
        println("/\\"*50)
    }  
  }

}

