package sources

import org.apache.spark.sql.{DataFrame, SaveMode}
import sources.parquet.ParquetDataMetadata

trait ParquetWritable {
  def writeParquet(df: DataFrame, parquetSourceMetadata: ParquetDataMetadata, saveMode: SaveMode): Unit
}
