package adventureworks.layers.workload.domain.dimension

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class StateProvince(
    stateprovinceid: Option[Int],
    stateprovincecode: String,
    countryregioncode: String,
    isonlystateprovinceflag: Option[Boolean],
    name: String,
    territoryid: Option[Int],
    rowguid: String,
    modifieddate: Timestamp
)

object StateProvince {
    def schema: StructType = {
        Encoders.product[StateProvince].schema
    }
}