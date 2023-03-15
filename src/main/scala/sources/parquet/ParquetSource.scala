package sources.parquet

import org.apache.spark.sql.{DataFrame, SparkSession}
import sources.{DataSource, DataMetadata}

class ParquetSource(spark: SparkSession) extends DataSource(spark) {
  private val inputFileNameRegex = "([^/]+.parquet)".r

  def read(parquetSourceMetadata: DataMetadata): DataFrame = {
    val tableMetadata = parquetSourceMetadata.asInstanceOf[ParquetDataMetadata]
    spark.read
      .parquet(tableMetadata.qualifiedName)
  }

    def inputFile(df: DataFrame): String = {
      inputFileNameRegex.findFirstIn(df.inputFiles(0)).get
    }

}
