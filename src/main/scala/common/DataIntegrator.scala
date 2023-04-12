package common

import org.apache.spark.sql.SparkSession
import sources.parquet.ParquetSource

abstract class DataIntegrator {
  def integrate(spark: SparkSession): Unit

  protected def parquetSource: ParquetSource.type = DataIntegrator.parquetSource
}

object DataIntegrator {
  val parquetSource: ParquetSource.type = ParquetSource
}
