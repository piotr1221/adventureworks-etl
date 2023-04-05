package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object SpecialOfferDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "specialofferid")
      && stringNotEmpty(df, "description")
      && df("discountpct").between(0, 1)
      && stringNotEmpty(df, "type")
      && stringNotEmpty(df, "category")
      && df("startdate") < df("enddate")
      && isPositiveOrZero(df, "minqty")
    ).where(
      """
        |CASE
        | WHEN maxqty IS NOT NULL THEN minqty <= maxqty
        | ELSE true
        |END""".stripMargin
    )
}
