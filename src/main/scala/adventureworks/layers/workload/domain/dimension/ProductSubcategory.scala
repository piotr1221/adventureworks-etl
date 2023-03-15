package adventureworks.layers.workload.domain.dimension

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class ProductSubcategory(
    productsubcategoryid: Option[Int],
    productcategoryid: Option[Int],
    name: String,
    rowguid: String,
    modifieddate: Timestamp
)

object ProductSubcategory {
    def schema: StructType = {
        Encoders.product[ProductSubcategory].schema
    }
}
