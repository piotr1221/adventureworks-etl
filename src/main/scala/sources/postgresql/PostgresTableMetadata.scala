package sources.postgresql

import org.apache.spark.sql.types.StructType
import sources.DataMetadata

final case class PostgresTableMetadata(
    dbSchema: String,
    table: String,
    dataFrameSchema: StructType) extends DataMetadata(table) {
    def qualifiedName: String = s"${dbSchema}.${table}"
}
