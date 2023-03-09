package layers.landing.domain.dimension

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class CountryRegion(
    countryregioncode: String,
    name: String,
    modifieddate: Timestamp
)

object CountryRegion {
    def schema: StructType = {
        Encoders.product[CountryRegion].schema
    }
}