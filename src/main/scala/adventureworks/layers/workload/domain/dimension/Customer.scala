package adventureworks.layers.workload.domain.dimension

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class Customer(
    customerid: Option[Int],
    personid: Option[Int],
    storeid: Option[Int],
    territoryid: Option[Int],
    rowguid: String,
    modifieddate: Timestamp
)

object Customer {
    def schema: StructType = {
        Encoders.product[Customer].schema
    }
}
