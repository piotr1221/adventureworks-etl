package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object AddressDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame = {
    df.where(
//      df("addressline1").isNotNull && !df("addressline1").rlike(DataCleaner.EMPTY_STRING_REGEX)
//      && df("city").isNotNull && !df("city").rlike(DataCleaner.EMPTY_STRING_REGEX)
//      && df("stateprovinceid").isNotNull && df("stateprovinceid") > 0
//      && df("postcalcode").isNotNull && !df("postalcode").rlike(DataCleaner.EMPTY_STRING_REGEX)
      stringNotEmpty(df, "addressline1")
      && stringNotEmpty(df, "city")
      && isPositive(df, "stateprovinceid")
      && stringNotEmpty(df, "postalcode")
    )
  }
}
