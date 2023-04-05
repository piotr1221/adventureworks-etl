package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object ProductSubcategoryDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "productsubcategoryid")
      && isPositive(df, "productcategoryid")
      && stringNotEmpty(df, "name")
    )
}
