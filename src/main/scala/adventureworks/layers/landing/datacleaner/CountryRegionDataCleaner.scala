package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object CountryRegionDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      stringNotEmpty(df, "countryregioncode")
      && stringNotEmpty(df, "name")
    )
}
