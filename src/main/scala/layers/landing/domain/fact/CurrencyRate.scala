package layers.landing.domain.fact

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class CurrencyRate(
    currencyrateid: Long,
    currencyratedate: Timestamp,
    fromcurrencycode: String,
    tocurrencycode: String,
    averagerate: BigDecimal,
    endofdayrate: BigDecimal,
    modifieddate: Timestamp
)

object CurrencyRate {
    def schema: StructType = {
        Encoders.product[CurrencyRate].schema
    }
}