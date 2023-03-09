package layers.landing.domain.dimension

import java.sql.Timestamp
import scala.math.BigDecimal
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class SalesPerson(
    businessentityid: Option[Int],
    territoryid: Option[Int],
    salesquota: BigDecimal,
    bonus: BigDecimal,
    commissionpct: BigDecimal,
    salesytd: BigDecimal,
    saleslastyear: BigDecimal,
    rowguid: String,
    modifieddate: Timestamp
)

object SalesPerson {
    def schema: StructType = {
        Encoders.product[SalesPerson].schema
    }
}