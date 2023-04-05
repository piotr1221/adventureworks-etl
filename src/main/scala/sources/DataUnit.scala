package sources

import datacleaner.DataCleaner
import org.apache.spark.sql.types.StructType
import sources.parquet.ParquetDataUnit

abstract class DataUnit(
  val table: String,
  val schema: Option[StructType],
  val dataCleaner: Option[DataCleaner]
) {
  def qualifiedName: String = table

  def toParquetDataUnit(layer: DataLakeLayer = Workload): ParquetDataUnit = {
    new ParquetDataUnit(layer.path, table)
  }
}