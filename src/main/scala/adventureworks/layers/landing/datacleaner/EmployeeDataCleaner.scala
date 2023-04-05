package adventureworks.layers.landing.datacleaner

import datacleaner.DataCleaner
import org.apache.spark.sql.DataFrame

object EmployeeDataCleaner extends DataCleaner {
  override def clean(df: DataFrame): DataFrame =
    df.where(
      isPositive(df, "businessentityid")
      && stringNotEmpty(df, "nationalidnumber")
      && stringNotEmpty(df, "jobtitle")
      && isPositiveOrZero(df, "vacationhours")
      && isPositiveOrZero(df, "sickleavehours")
    )
}
