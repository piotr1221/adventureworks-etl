package datacleaner

import common.Orchestrator
import datacleaner.DataCleanerOrchestrator.parquetSource
import org.apache.spark.sql.SparkSession
import sources.parquet.ParquetSource
import sources.{DataUnit, Landing}

class DataCleanerOrchestrator extends Orchestrator {
  def start(spark: SparkSession, dataUnits: Vector[DataUnit]): Unit = {
    dataUnits.foreach(dataUnit => {
      dataUnit.dataCleaner match {
        case Some(dataCleaner: DataCleaner) =>
          val cleanedDf = dataCleaner.clean(parquetSource.read(spark, dataUnit))
          parquetSource.writeParquet(cleanedDf, dataUnit.toParquetDataUnit(Landing))
        case _ => println(s"DataCleaner for table ${dataUnit.qualifiedName} missing")
      }
    })
  }
}

object DataCleanerOrchestrator {
  val parquetSource: ParquetSource.type = ParquetSource
}
