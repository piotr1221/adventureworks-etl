package common

import common.curated.SalesFactDataIntegrator
import datacleaner.{DataCleaner, DataCleanerOrchestrator}
import org.apache.spark.sql.SparkSession
import sources.{DataSource, DataSourceOrchestrator}

class PipelineOrchestrator {
  private var spark: SparkSession = _
  private var dataSourceOrchestrators: Array[DataSourceOrchestrator] = _
  private var dataCleanerOrchestrators: Array[DataCleanerOrchestrator] = _

  def start(sourceUnits: Array[SourceUnit], dataIntegrators: Vector[DataIntegrator]): Unit = {
//    workload(sourceUnits)
//    landing(sourceUnits)
    curated(dataIntegrators)
  }

  private def workload(sourceUnits: Array[SourceUnit]): Unit = sourceUnits.foreach(_.extractDataFromSources(spark))
  private def landing(sourceUnits: Array[SourceUnit]): Unit = sourceUnits.foreach(_.cleanData(spark))
  private def curated(dataIntegrators: Vector[DataIntegrator]): Unit = dataIntegrators.foreach(_.integrate(spark))

  /* Setters for ETLOrchestratorBuilder */
  protected def setSpark(spark: SparkSession): Unit = this.spark = spark
  protected def setDataSourceOrchestrators(dataSourceOrchestrators: Array[DataSourceOrchestrator]): Unit =
    this.dataSourceOrchestrators = dataSourceOrchestrators
  protected def setDataCleanerOrchestrators(dataCleanerOrchestrators: Array[DataCleanerOrchestrator]): Unit =
    this.dataCleanerOrchestrators = dataCleanerOrchestrators

}

object PipelineOrchestrator {
  def builder = new ETLOrchestratorBuilder()

  class ETLOrchestratorBuilder {
    private val etlOrchestrator = new PipelineOrchestrator()

    def sparkSession(spark: SparkSession): ETLOrchestratorBuilder = {
      etlOrchestrator.setSpark(spark)
      this
    }

    def dataSourceOrchestrators(dataSourceOrchestrators: Array[DataSourceOrchestrator]): ETLOrchestratorBuilder = {
      etlOrchestrator.setDataSourceOrchestrators(dataSourceOrchestrators)
      this
    }

    def dataCleanerOrchestrators(dataCleanerOrchestrators: Array[DataCleanerOrchestrator]): ETLOrchestratorBuilder = {
      etlOrchestrator.setDataCleanerOrchestrators(dataCleanerOrchestrators)
      this
    }

    def build(): PipelineOrchestrator = this.etlOrchestrator
  }
}