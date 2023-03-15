package adventureworks.layers.workload.domain.fact

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class SalesOrderDetail(
    salesorderid: Option[Int],
    salesorderdetailid: Option[Int],
    carriertrackingnumber: String,
    orderqty: Option[Short],
    productid: Option[Int],
    specialofferid: Option[Int],
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