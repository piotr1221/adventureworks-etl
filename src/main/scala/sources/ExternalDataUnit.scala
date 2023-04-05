package sources

import datacleaner.DataCleaner
import org.apache.spark.sql.types.StructType
import sources.parquet.ParquetDataUnit

abstract class ExternalDataUnit(
  project: String,
  table: String,
  schema: Option[StructType],
  dataCleaner: Option[DataCleaner])
  extends DataUnit(table, schema, dataCleaner) {
  override def toParquetDataUnit(layer: DataLakeLayer = Workload): ParquetDataUnit = {
    new ParquetDataUnit(layer.path, s"${project}/${table}")
  }
}
