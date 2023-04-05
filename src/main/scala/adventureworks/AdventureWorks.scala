package adventureworks

import adventureworks.layers.landing.datacleaner._
import adventureworks.layers.workload.domain.dimension._
import adventureworks.layers.workload.domain.fact._
import common.{Orchestrator, SourceUnit}
import common.OrchestratorEnum._
import datacleaner.DataCleanerOrchestrator
import sources.{DataSourceOrchestrator, DataUnit}
import sources.postgresql.{PostgresDataUnit, PostgresSource}

object AdventureWorks {
  val project = "adventureworks"

  def dataSourceOrchestrator: DataSourceOrchestrator = new AdventureWorksDataSourceOrchestrator
  def dataCleanerOrchestrator: DataCleanerOrchestrator = new DataCleanerOrchestrator

  def setup(): SourceUnit = {
    new SourceUnit(
      dataSource,
      dataUnit,
      orchestrators
    )
  }

  private def orchestrators: Map[Value, Orchestrator] =  {
    Map(
      Source -> dataSourceOrchestrator,
      Cleaner -> dataCleanerOrchestrator
    )
  }

  private def dataSource = new PostgresSource(
    "jdbc",
    "org.postgresql.Driver",
    "jdbc:postgresql://localhost:5432/Adventureworks",
    "postgres",
    "postgres"
  )

  private def dataUnit = {
    Array[DataUnit](
      PostgresDataUnit(project, "person", "address", Option(Address.schema), Option(AddressDataCleaner)),
      PostgresDataUnit(project, "person", "countryregion", Option(CountryRegion.schema), Option(CountryRegionDataCleaner)),
      PostgresDataUnit(project, "sales", "customer", Option(Customer.schema), Option(CustomerDataCleaner)),
      PostgresDataUnit(project, "humanresources", "employee", Option(Employee.schema), Option(EmployeeDataCleaner)),
      PostgresDataUnit(project, "person", "person", Option(Person.schema), Option(PersonDataCleaner)),
      PostgresDataUnit(project, "production", "product", Option(Product.schema), Option(ProductDataCleaner)),
      PostgresDataUnit(project, "production", "productcategory", Option(ProductCategory.schema), Option(ProductCategoryDataCleaner)),
      PostgresDataUnit(project, "production", "productsubcategory", Option(ProductSubcategory.schema), Option(ProductCategoryDataCleaner)),
      PostgresDataUnit(project, "sales", "salesperson", Option(SalesPerson.schema), Option(SalesPersonDataCleaner)),
      PostgresDataUnit(project, "sales", "salesterritory", Option(SalesTerritory.schema), Option(SalesTerritoryDataCleaner)),
      PostgresDataUnit(project, "sales", "specialoffer", Option(SpecialOffer.schema), Option(SpecialOfferDataCleaner)),
      PostgresDataUnit(project, "sales", "specialofferproduct", Option(SpecialOfferProduct.schema), Option(SpecialOfferProductDataCleaner)),
      PostgresDataUnit(project, "person", "stateprovince", Option(StateProvince.schema), Option(StateProvinceDataCleaner)),
      PostgresDataUnit(project, "sales", "store", Option(Store.schema), Option(StoreDataCleaner)),
      PostgresDataUnit(project, "sales", "currencyrate", Option(CurrencyRate.schema), Option(CurrencyRateDataCleaner)),
      PostgresDataUnit(project, "sales", "salesorderdetail", Option(SalesOrderDetail.schema), Option(SalesOrderDetailDataCleaner)),
      PostgresDataUnit(project, "sales", "salesorderheader", Option(SalesOrderHeader.schema), Option(SalesOrderHeaderDataCleaner)),
      PostgresDataUnit(project, "sales", "salestaxrate", Option(SalesTaxRate.schema),Option(SalesTaxRateDataCleaner))
    )
  }
}
