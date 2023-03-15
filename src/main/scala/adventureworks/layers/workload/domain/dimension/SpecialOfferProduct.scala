package adventureworks.layers.workload.domain.dimension

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class SpecialOfferProduct(
    specialofferid: Option[Int],
    productid: Option[Int],
    rowguid: String,
    modifieddate: Timestamp
)

object SpecialOfferProduct {
    def schema: StructType = {
        Encoders.product[SpecialOfferProduct].schema
    }
}