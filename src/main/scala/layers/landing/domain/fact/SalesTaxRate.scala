package layers.landing.domain.fact

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class SalesTaxRate(
    salestaxrateid: Long,
    stateprovinceid: Long,
    taxtype: Short,
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