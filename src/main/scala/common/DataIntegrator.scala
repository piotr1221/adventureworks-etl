package common

import org.apache.spark.sql.{DataFrame, SparkSession}
import sources.parquet.ParquetSource

abstract class DataIntegrator {
  def integrate(spark: SparkSession): Unit

  protected def parquetSource: ParquetSource.type = DataIntegrator.parquetSource

//  def columnsSeparatedByComma(df: DataFrame, tableName: String, sep: String = ",\n   "): String =
//    s"${df.columns.map(column => s"${tableName}.${column}").mkString(sep)} "

//  def columns(query: String, columnsMap: Map[String, Ra]): Map[String, Ra] = {
//    columnsMap.keys.foreach(key => {
//      val substr = query.substring(0, query.indexOf(key)-2).trim
//      val count = substr.count(_ == '\n')
//      val substrSplitByNewLine = substr.split("\n")(count)
////      println(count)
////      println(substrSplitByNewLine)
//      val blankSpacesCount = substrSplitByNewLine.substring(0, substrSplitByNewLine.indexWhere(_ != ' ')).length
////      println(blankSpacesCount)
//      columnsMap(key).blankSpaces = blankSpacesCount
//    })
//    columnsMap
//  }

//  protected class Ra(val columns: Vector[String], val tabs: Int = 0, val indentation: Int = 4) {
//    var blankSpaces = 0
//  }

}

object DataIntegrator {
  val parquetSource: ParquetSource.type = ParquetSource
}
