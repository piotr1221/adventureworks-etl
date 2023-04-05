package sources.parquet

import org.apache.spark.sql.{DataFrame, SparkSession}
import sources.{DataSource, DataUnit}

object ParquetSource extends DataSource {
  private val inputFileNameRegex = "([^/]+.parquet)".r

  override def read(spark: SparkSession, parquetDataUnit: DataUnit): DataFrame = {
    val dataUnit: ParquetDataUnit = parquetDataUnit match {
      case p: ParquetDataUnit => p
      case _ => parquetDataUnit.toParquetDataUnit()
    }
    spark.read
      .parquet(dataUnit.qualifiedName)
  }

  def inputFile(df: DataFrame): String = {
      inputFileNameRegex.findFirstIn(df.inputFiles(0)).get
    }
}
