package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object CurrencyRateDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "currencyrateid")
      && stringNotEmpty(df, "fromcurrencycode")
      && stringNotEmpty(df, "tocurrencycode")
      && isPositive(df, "averagerate")
      && isPositive(df, "endofdayrate")
    )
}
