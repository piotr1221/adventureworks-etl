package sources

import common.{Orchestrator, SourceUnit}
import org.apache.spark.sql.SparkSession

class DataSourceOrchestrator extends Orchestrator {
  def start(spark: SparkSession, dataSource: DataSource, dataUnits: Array[DataUnit]): Unit = {
    dataSource.readAndWriteInBulk(spark, dataUnits)
  }
}
