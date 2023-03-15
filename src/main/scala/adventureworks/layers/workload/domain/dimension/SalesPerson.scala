package adventureworks.layers.workload.domain.dimension

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp

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