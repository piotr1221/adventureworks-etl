package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object SalesOrderHeaderDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "salesorderid")
      && isPositive(df, "revisionnumber")
      && df("orderdate") < df("duedate")
      && isPositive(df, "status")
      && stringNotEmpty(df, "purchaseordernumber")
      && stringNotEmpty(df, "accountnumber")
      && isPositive(df, "customerid")
      && isPositive(df, "salespersonid")
      && isPositive(df, "territoryid")
      && isPositive(df, "billtoaddressid")
      && isPositive(df, "shiptoaddressid")
      && isPositive(df, "shipmethodid")
      && isPositive(df, "creditcardid")
      && stringNotEmpty(df, "creditcardapprovalcode")
      && isPositive(df, "subtotal")
      && isPositive(df, "taxamt")
      && isPositive(df, "freight")
      && isPositive(df, "totaldue")
    )
}
