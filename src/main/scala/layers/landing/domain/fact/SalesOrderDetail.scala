package layers.landing.domain.fact

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class SalesOrderDetail(
    salesorderid: Long,
    salesorderdetailid: Long,
    carriertrackingnumber: String,
    orderqty: Short,
    productid: Long,
    specialofferid: Long,
    unitprice: BigDecimal,
    unitpricediscount: BigDecimal,
    rowguid: String,
    modifieddate: Timestamp
)

object SalesOrderDetail {
    def schema: StructType = {
        Encoders.product[SalesOrderDetail].schema
    }
}