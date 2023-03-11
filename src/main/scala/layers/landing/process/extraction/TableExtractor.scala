package layers.landing.process.extraction

import org.apache.spark.sql.SparkSession

abstract class TableExtractor(
    spark: SparkSession
) {
    val landingLayerPath = "F:/Projects/ETL/AdventureWorks/datalake/landing"
    
    def startExtraction(): Unit

    /**
    * Concats the landing layer path in TableExtractor to the
    * specified table name, as:
    * @return landingLayerPath/table.parquet
    */
    def concatTableNameToLandingPathAsParquet(table: String) = 
        s"${landingLayerPath}/${table}.parquet"
}