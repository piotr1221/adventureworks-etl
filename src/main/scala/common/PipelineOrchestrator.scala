package common

import org.apache.spark.sql.SparkSession
import sources.DataSourceOrchestrator
import datacleaner.DataCleanerOrchestrator


class PipelineOrchestrator {
  private var spark: SparkSession = _
  private var dataSourceOrchestrators: Vector[DataSourceOrchestrator] = _
  private var dataCleanerOrchestrators: Vector[DataCleanerOrchestrator] = _

  def start(sourceUnits: Vector[SourceUnit], dataIntegrators: Vector[DataIntegrator]): Unit = {
    workload(sourceUnits)
    landing(sourceUnits)
    curated(dataIntegrators)
  }

  private def workload(sourceUnits: Vector[SourceUnit]): Unit = sourceUnits.foreach(_.extractDataFromSources(spark))
  private def landing(sourceUnits: Vector[SourceUnit]): Unit = sourceUnits.foreach(_.cleanData(spark))
  private def curated(dataIntegrators: Vector[DataIntegrator]): Unit = dataIntegrators.foreach(_.integrate(spark))

  /* Setters for ETLOrchestratorBuilder */
  protected def setSpark(spark: SparkSession): Unit = this.spark = spark
  protected def setDataSourceOrchestrators(dataSourceOrchestrators: Vector[DataSourceOrchestrator]): Unit =
    this.dataSourceOrchestrators = dataSourceOrchestrators
  protected def setDataCleanerOrchestrators(dataCleanerOrchestrators: Vector[DataCleanerOrchestrator]): Unit =
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

    def dataSourceOrchestrators(dataSourceOrchestrators: Vector[DataSourceOrchestrator]): ETLOrchestratorBuilder = {
      etlOrchestrator.setDataSourceOrchestrators(dataSourceOrchestrators)
      this
    }

    def dataCleanerOrchestrators(dataCleanerOrchestrators: Vector[DataCleanerOrchestrator]): ETLOrchestratorBuilder = {
      etlOrchestrator.setDataCleanerOrchestrators(dataCleanerOrchestrators)
      this
    }

    def build(): PipelineOrchestrator = this.etlOrchestrator
  }
}