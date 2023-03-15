package adventureworks.layers.workload.domain.dimension

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class SpecialOffer(
    specialofferid: Option[Int],
    description: String,
    discountpct: BigDecimal,
    `type`: String,
    category: String,
    startdate: Timestamp,
    enddate: Timestamp,
    minqty: Option[Int],
    maxqty: Option[Int],
    rowguid: String,
    modifieddate: Timestamp
)

object SpecialOffer {
    def schema: StructType = {
        Encoders.product[SpecialOffer].schema
    }
}