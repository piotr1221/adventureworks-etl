package layers.landing.domain.dimension

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class ProductCategory(
    productcategoryid: Option[Int],
    name: String,
    rowguid: String,
    modifieddate: Timestamp
)

object ProductCategory {
    def schema: StructType = {
        Encoders.product[ProductCategory].schema
    }
}