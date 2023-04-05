package sources

import org.apache.spark.sql.{DataFrame, SparkSession}

trait TableReader {

  def read(spark: SparkSession, tableMetadata: DataUnit): DataFrame

}
