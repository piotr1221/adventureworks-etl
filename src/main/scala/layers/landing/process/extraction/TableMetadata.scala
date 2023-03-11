package layers.landing.process.extraction

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types.StructType

final case class TableMetadata(
    dbSchema: String,
    table: String,
    dataFrameSchema: StructType) {
    def qualifiedName: String = s"${dbSchema}.${table}"
}
