package sources

import sources.parquet.ParquetDataMetadata

abstract class DataMetadata(
  table: String
) {
  def getName: String = table

  def toParquetSourceMetadata(layer: DataLakeLayer = Workload): ParquetDataMetadata = {
    ParquetDataMetadata(layer.path, table)
  }
}
