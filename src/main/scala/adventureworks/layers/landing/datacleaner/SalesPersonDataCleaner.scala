package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object SalesPersonDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "businessentityid")
      && isPositiveOrZero(df, "salesquota")
      && isPositiveOrZero(df, "bonus")
      && isPositiveOrZero(df, "commissionpct")
      && isPositiveOrZero(df, "salesytd")
      && isPositiveOrZero(df, "saleslastyear")
    )
}
