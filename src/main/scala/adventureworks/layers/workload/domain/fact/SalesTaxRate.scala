package adventureworks.layers.workload.domain.fact

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class SalesTaxRate(
    salestaxrateid: Option[Int],
    stateprovinceid: Option[Int],
    taxtype: Option[Short],
    taxrate: BigDecimal,
    name: String,
    rowguid: String,
    modifieddate: Timestamp
)

object SalesTaxRate {
    def schema: StructType = {
        Encoders.product[SalesTaxRate].schema
    }
}