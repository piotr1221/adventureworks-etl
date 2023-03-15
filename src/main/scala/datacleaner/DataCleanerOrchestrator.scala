package datacleaner

import adventureworks.layers.landing.datacleaner.AddressDataCleaner
import org.apache.spark.sql.SparkSession
import sources.parquet.{ParquetDataMetadata, ParquetSource}
import sources.Landing

class DataCleanerOrchestrator(spark: SparkSession, dataCleaners: Array[DataCleaner]) { //extends Orchestrator {
  val parquetSource: ParquetSource = new ParquetSource(spark)

  def start(parquetDataMetadataArray: Array[ParquetDataMetadata]): Unit = {
    for (parquetDataMetadata <- parquetDataMetadataArray) {
      val dataCleaner = getDataCleaner(parquetDataMetadata.fileName)
      val df = dataCleaner.clean(parquetSource.read(parquetDataMetadata))
      parquetSource.writeParquet(df, parquetDataMetadata.toParquetSourceMetadata(Landing))
    }

//    for (dataCleaner <- dataCleaners) {
//      val df = dataCleaner.clean()
//      parquetSource.writeParquet(df, parquetSource.getParquetSourceMetadata(parquetSource.inputFile(df), Landing))
//    }
  }

  private def getDataCleaner(name: String): DataCleaner = {
    name match {
      case "address" => AddressDataCleaner
    }
  }

}
