package adventureworks.layers.workload.domain.dimension

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp

final case class SalesTerritory(
    territoryid: Option[Int],
    name: String,
    countryregioncode: String,
    group: String,
    salesytd: BigDecimal,
    saleslastyear: BigDecimal,
    costytd: BigDecimal,
    costlastyear: BigDecimal,
    rowguid: String,
    modifieddate: Timestamp
)

object SalesTerritory {
    def schema: StructType = {
        Encoders.product[SalesTerritory].schema
    }
}