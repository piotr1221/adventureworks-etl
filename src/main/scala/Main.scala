import adventureworks.AdventureWorks
import org.apache.spark.sql.SparkSession
import common.PipelineOrchestrator
import sources.DataUnit
import sources.parquet.ParquetDataUnit

object Main {  
  def main(args: Array[String]): Unit = {
    val pipelineOrchestrator = PipelineOrchestrator.builder
      .sparkSession(sparkSession)
      .build()

    pipelineOrchestrator.start(
      Array(AdventureWorks.setup())
    )

  }

  def sparkSession: SparkSession = {
    SparkSession.builder()
      .appName("AdventureWorks ETL")
      .getOrCreate()
  }

  def sourceMetadataToParquet(dataMetadataArray: Array[DataUnit]): Array[ParquetDataUnit] = {
    dataMetadataArray.map(_.toParquetDataUnit())
  }

}