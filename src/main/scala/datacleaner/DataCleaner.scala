package datacleaner

import org.apache.spark.sql.DataFrame

abstract class DataCleaner {
  def clean(df: DataFrame): DataFrame
}

object DataCleaner {
  val EMPTY_STRING_REGEX = "^[\\s]*$"
}