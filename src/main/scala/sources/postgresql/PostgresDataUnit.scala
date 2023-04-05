package sources.postgresql

import datacleaner.DataCleaner
import org.apache.spark.sql.types.StructType
import sources.ExternalDataUnit

final case class PostgresDataUnit(
                                   project: String,
                                   dbSchema: String,
                                   override val table: String,
                                   override val schema: Option[StructType],
                                   override val dataCleaner: Option[DataCleaner])
    extends ExternalDataUnit(project, table, schema, dataCleaner) {
    override def qualifiedName: String = s"${dbSchema}.${table}"
}
