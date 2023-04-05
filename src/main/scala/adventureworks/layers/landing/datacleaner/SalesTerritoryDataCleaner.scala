package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object SalesTerritoryDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "territoryid")
      && stringNotEmpty(df, "name")
      && stringNotEmpty(df, "countryregioncode")
      && stringNotEmpty(df, "group")
      && isPositiveOrZero(df, "salesytd")
      && isPositiveOrZero(df, "saleslastyear")
      && isPositiveOrZero(df, "costytd")
      && isPositiveOrZero(df, "costlastyear")
    )
}
