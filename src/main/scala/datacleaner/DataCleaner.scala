package datacleaner

import org.apache.spark.sql.{Column, DataFrame}

abstract class DataCleaner {
  def clean(df: DataFrame): DataFrame

  protected def stringNotEmpty(df: DataFrame, column: String): Column =
    df(column).isNotNull && !df(column).rlike(DataCleaner.EMPTY_STRING_REGEX)

  protected def isPositive(df: DataFrame, column: String): Column =
    df(column).isNotNull && df(column) > 0

  protected def isPositiveOrZero(df: DataFrame, column: String): Column =
    df(column).isNotNull && df(column) >= 0
}

object DataCleaner {
  val EMPTY_STRING_REGEX = "^[\\s]*$"
}