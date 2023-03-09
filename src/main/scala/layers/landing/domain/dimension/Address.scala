package layers.landing.domain.dimension

import java.sql.Timestamp
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

final case class Address(
    addressid: Option[Int],
    addressline1: String,
    addressline2: String,
    city: String,
    stateprovinceid: Option[Int],
    postalcode: String,
    spatiallocation: Array[Byte],
    rowguid: String,
    modifieddate: Timestamp
)

object Address {
    def schema: StructType = {
        Encoders.product[Address].schema
    }
}