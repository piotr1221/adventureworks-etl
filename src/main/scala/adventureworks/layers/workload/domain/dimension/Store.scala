package adventureworks.layers.workload.domain.dimension

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class Store(
    businessentityid: Option[Int],
    name: String,
    salespersonid: Option[Int],
    demographics: String,
    rowguid: String,
    modifieddate: Timestamp
)

object Store {
    def schema: StructType = {
        Encoders.product[Store].schema
    }
}