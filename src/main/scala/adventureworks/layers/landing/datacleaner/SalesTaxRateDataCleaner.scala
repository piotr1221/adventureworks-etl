package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object SalesTaxRateDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "salestaxrateid")
      && isPositive(df, "stateprovinceid")
      && isPositive(df, "taxtype")
      && isPositive(df, "taxrate")
      && stringNotEmpty(df, "name")
    )
}
