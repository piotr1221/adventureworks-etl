package sources

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

abstract class DataSource extends TableReader with ParquetWriter {

    def read(spark: SparkSession, dataUnit: DataUnit): DataFrame

    override def writeParquet(df: DataFrame,
                              dataUnit: DataUnit,
                              saveMode: SaveMode = SaveMode.Overwrite): Unit = {
        df.write
          .format("parquet")
          .mode(saveMode)
          .save(
              dataUnit.qualifiedName
          )
    }

    def readAndWriteInBulk(spark: SparkSession, dataUnits: Vector[DataUnit]): Unit = {
        for (dataUnit <- dataUnits) {
            println ("/\\" * 50)
            println ("::::::::::::::::::::: " + dataUnit.qualifiedName + " :::::::::::::::::::::")

            val df = read(spark, dataUnit)
            writeParquet(df, dataUnit.toParquetDataUnit())

            println("-*" * 50)
        }
    }
}