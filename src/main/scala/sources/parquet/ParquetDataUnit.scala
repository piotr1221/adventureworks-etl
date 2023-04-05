package sources.parquet

import datacleaner.DataCleaner
import org.apache.spark.sql.types.StructType
import sources.{DataLakeLayer, DataUnit, Workload, Landing, Curated, Functional}

case class ParquetDataUnit(
  folderPath: String,
  fileName: String,
  override val schema: Option[StructType],
  override val dataCleaner: Option[DataCleaner]
) extends DataUnit(fileName, schema, dataCleaner) {

  def this(folderPath: String, fileName: String) {
    this(folderPath, fileName, None, None)
  }

  def this(folderPath: String, project: String, fileName: String) {
    this(folderPath, s"${project}/${fileName}")
  }

  override def qualifiedName: String = s"${folderPath}/${fileName}.parquet"

  override def toParquetDataUnit(layer: DataLakeLayer = Workload): ParquetDataUnit = {
    if (folderPath == layer.path) return this

    super.toParquetDataUnit(layer)
  }
}

case object ParquetDataUnit {
  def workload(project: String, fileName: String) = new ParquetDataUnit(Workload.path, project, fileName)
  def workload(fileName: String) = new ParquetDataUnit(Workload.path, fileName)

  def landing(project: String, fileName: String) = new ParquetDataUnit(Landing.path, project, fileName)
  def landing(fileName: String) = new ParquetDataUnit(Landing.path, fileName)

  def curated(project: String, fileName: String) = new ParquetDataUnit(Curated.path, project, fileName)
  def curated(fileName: String) = new ParquetDataUnit(Curated.path, fileName)

  def functional(project: String, fileName: String) = new ParquetDataUnit(Functional.path, project, fileName)
  def functional(fileName: String) = new ParquetDataUnit(Functional.path, fileName)
}
