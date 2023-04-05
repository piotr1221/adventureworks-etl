package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object CustomerDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "customerid")
      && isPositive(df, "storeid")
      && isPositive(df, "territoryid")
    )
}
