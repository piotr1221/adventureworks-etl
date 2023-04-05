package common

import org.apache.spark.sql.{DataFrame, SparkSession}
import sources.parquet.ParquetSource

abstract class DataIntegrator {
  def integrate(spark: SparkSession): Unit

  def parquetSource: ParquetSource.type = DataIntegrator.parquetSource

  def columnsSeparatedByComma(df: DataFrame, tableName: String, sep: String = ",\n   "): String =
    s"${df.columns.map(column => s"${tableName}.${column}").mkString(sep)} "
}

object DataIntegrator {
  val parquetSource: ParquetSource.type = ParquetSource
}
