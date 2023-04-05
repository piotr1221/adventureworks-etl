package sources.postgresql

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.sql.types.StructType
import sources.{DataUnit, DataSource}

class PostgresSource(
                              format: String,
                              driver: String,
                              url: String,
                              user: String,
                              password: String
                            ) extends DataSource {
  override def read(spark: SparkSession, postgresDataUnit: DataUnit): DataFrame = {
    val dataUnit = postgresDataUnit.asInstanceOf[PostgresDataUnit]

    dataUnit.schema match {
      case Some(schema: StructType) => read(spark, dataUnit.qualifiedName, schema)
      case None => read(spark, dataUnit.qualifiedName)
    }
  }

  def read(spark: SparkSession, dbtable: String, schema: StructType): DataFrame = {
    getDataFrameReader(spark, dbtable)
      .schema(schema)
      .load()
  }

  def read(spark: SparkSession, dbtable: String): DataFrame = {
    getDataFrameReader(spark, dbtable)
      .load()
  }

  private def getDataFrameReader(spark: SparkSession, dbtable: String): DataFrameReader = {
    spark.read
      .format(format)
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", dbtable)
      .option("user", user)
      .option("password", password)
  }

}

