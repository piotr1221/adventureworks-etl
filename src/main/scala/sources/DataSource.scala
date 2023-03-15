package sources

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import sources.parquet.ParquetDataMetadata

abstract class DataSource(
    spark: SparkSession
) extends ParquetWritable {

    def read(tableMetadata: DataMetadata): DataFrame

    def getParquetSourceMetadata(fileName: String, layer: DataLakeLayer) = {
        ParquetDataMetadata(layer.path, fileName)
    }

    override def writeParquet(df: DataFrame, parquetSourceMetadata: ParquetDataMetadata, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
        df.write
          .format("parquet")
          .mode(saveMode)
          .save(parquetSourceMetadata.qualifiedName)
    }
}