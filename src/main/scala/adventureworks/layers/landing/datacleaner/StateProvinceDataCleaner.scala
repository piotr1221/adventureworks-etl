package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object StateProvinceDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "stateprovinceid")
      && stringNotEmpty(df, "stateprovincecode")
      && stringNotEmpty(df, "countryregioncode")
      && stringNotEmpty(df, "name")
      && isPositive(df, "territoryid")
    )
}
