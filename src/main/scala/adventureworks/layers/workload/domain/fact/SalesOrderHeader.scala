package adventureworks.layers.workload.domain.fact

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class SalesOrderHeader(
    salesorderid: Option[Int],
    revisionnumber: Option[Short],
    orderdate: Timestamp,
    duedate: Timestamp,
    shipdate: Timestamp,
    status: Option[Short],
    onlineorderflag: Option[Boolean],
    purchaseordernumber: String,
    accountnumber: String,
    customerid: Option[Int],
    salespersonid: Option[Int],
    territoryid: Option[Int],
    billtoaddressid: Option[Int],
    shiptoaddressid: Option[Int],
    shipmethodid: Option[Int],
    creditcardid: Option[Int],
    creditcardapprovalcode: String,
    currencyrateid: Option[Int],
    subtotal: BigDecimal,
    taxamt: BigDecimal,
    freight: BigDecimal,
    totaldue: BigDecimal,
    comment: String,
    rowguid: String,
    modifieddate: Timestamp
)

object SalesOrderHeader {
    def schema: StructType = {
        Encoders.product[SalesOrderHeader].schema
    }
}