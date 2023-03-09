import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.immutable.Map

import layers.landing.process.TableExtractor

object Main {  
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    import spark.implicits._

    val tableExtractor = TableExtractor
    tableExtractor.start(spark)
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("AdventureWorks ETL")
      .getOrCreate()
  }

}