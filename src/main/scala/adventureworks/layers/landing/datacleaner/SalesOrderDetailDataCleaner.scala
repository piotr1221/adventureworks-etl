package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object SalesOrderDetailDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "salesorderid")
      && isPositive(df, "salesorderdetailid")
      && stringNotEmpty(df, "carriertrackingnumber")
      && isPositive(df, "orderqty")
      && isPositive(df, "productid")
      && isPositive(df, "specialofferid")
      && isPositive(df, "unitprice")
      && df("unitpricediscount").between(0, 1)
    )
}
