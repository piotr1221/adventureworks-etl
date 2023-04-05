package common

import org.apache.spark.sql.{DataFrame, SparkSession}
import common.OrchestratorEnum._
import datacleaner.DataCleanerOrchestrator
import sources.{DataSource, DataSourceOrchestrator, DataUnit, Landing}

class SourceUnit(
  val dataSource: DataSource,
  val dataUnits: Array[DataUnit],
  val orchestrators: Map[OrchestratorEnum.Value, _ <: Orchestrator]
) {
  def extractDataFromSources(spark: SparkSession): Unit = {
    orchestrators(Source).asInstanceOf[DataSourceOrchestrator].start(spark, dataSource, dataUnits)
  }

  def cleanData(spark: SparkSession): Unit = {
    orchestrators(Cleaner).asInstanceOf[DataCleanerOrchestrator].start(spark, dataUnits)
  }
}
