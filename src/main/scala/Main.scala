import adventureworks.AdventureWorks
import org.apache.spark.sql.SparkSession
import common.PipelineOrchestrator
import common.curated.{CustomerDataIntegrator, ProductDataIntegrator, SalesFactDataIntegrator, SalesPersonDataIntegrator}
import sources.DataUnit
import sources.parquet.ParquetDataUnit

object Main {  
  def main(args: Array[String]): Unit = {
    val pipelineOrchestrator = PipelineOrchestrator.builder
      .sparkSession(sparkSession)
      .build()

    val dataIntegrators = Vector(
//      new SalesFactDataIntegrator(),
//      new CustomerDataIntegrator(),
//      new SalesPersonDataIntegrator(),
      new ProductDataIntegrator()
    )

    pipelineOrchestrator.start(
      Array(AdventureWorks.setup()),
      dataIntegrators
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