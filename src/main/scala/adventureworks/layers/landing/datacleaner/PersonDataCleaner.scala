package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object PersonDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "businessentityid")
      && stringNotEmpty(df, "persontype")
      && stringNotEmpty(df, "firstname")
      && stringNotEmpty(df, "lastname")
      && isPositive(df, "emailpromotion")
    )
}
