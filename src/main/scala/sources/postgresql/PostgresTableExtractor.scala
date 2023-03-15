package sources.postgresql

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.sql.types.StructType
import sources.{DataMetadata, DataSource, Workload}

class PostgresTableExtractor(
                              spark: SparkSession,
                              format: String,
                              driver: String,
                              url: String,
                              user: String,
                              password: String
                            ) extends DataSource(spark) {
  override def read(postgresTableMetadata: DataMetadata): DataFrame = {
    val tableMetadata = postgresTableMetadata.asInstanceOf[PostgresTableMetadata]
    println ("/\\" * 50)
    println (":::::::::::::::::::::" + tableMetadata.table)

    read(tableMetadata.qualifiedName, tableMetadata.dataFrameSchema)
  }

  def extract(postgresTableMetadataArray: Array[DataMetadata]): Unit = {
    for (postgresTableMetadata <- postgresTableMetadataArray) {

      val df = read(postgresTableMetadata)

      persistTableInDatalake(df, postgresTableMetadata)
      println("-*" * 50)
    }
  }

  def read(dbtable: String, schema: StructType): DataFrame = {
    getDataFrameReader(dbtable)
      .schema(schema)
      .load()
  }


  def read(dbtable: String): DataFrame = {
    getDataFrameReader(dbtable)
      .load()
  }

  private def getDataFrameReader(dbtable: String): DataFrameReader = {
    spark.read
      .format(format)
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", dbtable)
      .option("user", user)
      .option("password", password)
  }


  private def persistTableInDatalake(df: DataFrame, dataMetadata: DataMetadata): Unit = {
    writeParquet(df, dataMetadata.toParquetSourceMetadata())
  }

}

