package sources

import org.apache.spark.sql.{DataFrame, SaveMode}
import sources.parquet.ParquetDataUnit

trait ParquetWriter {
  def writeParquet(df: DataFrame,
                   parquetSourceMetadata: DataUnit,
                   saveMode: SaveMode = SaveMode.Overwrite): Unit
}
