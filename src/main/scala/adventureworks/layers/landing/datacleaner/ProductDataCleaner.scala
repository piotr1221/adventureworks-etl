package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object ProductDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "productid")
      && stringNotEmpty(df, "name")
      && stringNotEmpty(df, "productnumber")
      && isPositive(df, "safetystocklevel")
      && isPositive(df, "reorderpoint")
      && isPositive(df, "standardcost")
      && isPositive(df, "listprice")
    )
}
