package layers.landing.domain.dimension

import scala.math.BigDecimal
import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

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