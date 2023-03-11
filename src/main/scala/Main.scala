import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.immutable.Map

import layers.landing.process.extraction.PostgresTableExtractor

object Main {  
  def main(args: Array[String]): Unit = {
    val tableExtractor = new PostgresTableExtractor(
      createSparkSession(),
      "jdbc",
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost:5432/Adventureworks",
      "postgres",
      "postgres"
    )
    tableExtractor.startExtraction()
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("AdventureWorks ETL")
      .getOrCreate()
  }

}