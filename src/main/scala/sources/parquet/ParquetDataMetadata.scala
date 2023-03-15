package sources.parquet

import sources.{DataLakeLayer, DataMetadata, Workload}

case class ParquetDataMetadata(
  folderPath: String,
  fileName: String
) extends DataMetadata(fileName) {
  def qualifiedName: String = s"${folderPath}/${fileName}.parquet"

  override def toParquetSourceMetadata(layer: DataLakeLayer = Workload): ParquetDataMetadata = {
    if (folderPath == layer.path) return this

    super.toParquetSourceMetadata(layer)
  }
}
