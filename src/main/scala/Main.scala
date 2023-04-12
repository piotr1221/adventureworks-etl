import adventureworks.AdventureWorks
import org.apache.spark.sql.SparkSession
import common.PipelineOrchestrator
import common.curated.DataIntegratorSetup

object Main {  
  def main(args: Array[String]): Unit = {
    val pipelineOrchestrator = PipelineOrchestrator.builder
      .sparkSession(sparkSession)
      .build()

    pipelineOrchestrator.start(
      Vector(AdventureWorks.setup()),
      DataIntegratorSetup.setup
    )

  }

  def sparkSession: SparkSession = {
    SparkSession.builder()
      .appName("AdventureWorks ETL")
      .getOrCreate()
  }
}