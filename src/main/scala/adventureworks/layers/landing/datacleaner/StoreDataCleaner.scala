package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object StoreDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "businessentityid")
      && stringNotEmpty(df, "name")
      && isPositive(df, "salespersonid")
    )
}
