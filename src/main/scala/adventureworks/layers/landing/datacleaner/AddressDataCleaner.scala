package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object AddressDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame = {
    df.where(
      stringNotEmpty(df, "addressline1")
      && stringNotEmpty(df, "city")
      && isPositive(df, "stateprovinceid")
      && stringNotEmpty(df, "postalcode")
    )
  }
}
