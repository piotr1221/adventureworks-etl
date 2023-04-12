package sources

import common.Orchestrator
import org.apache.spark.sql.SparkSession

class DataSourceOrchestrator extends Orchestrator {
  def start(spark: SparkSession, dataSource: DataSource, dataUnits: Vector[DataUnit]): Unit = {
    dataSource.readAndWriteInBulk(spark, dataUnits)
  }
}
