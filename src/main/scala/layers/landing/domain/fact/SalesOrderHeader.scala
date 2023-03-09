package layers.landing.domain.fact

import java.sql.Timestamp
import java.sql.Time
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class SalesOrderHeader(
    salesorderid: Long,
    revisionnumber: Short,
    orderdate: Timestamp,
    duedate: Timestamp,
    shipdate: Timestamp,
    status: Short,
    onlineorderflag: Boolean,
    purchaseordernumber: String,
    accountnumber: String,
    customerid: Long,
    salespersonid: Long,
    territoryid: Long,
    billtoaddressid: Long,
    shiptoaddressid: Long,
    shipmethodid: Long,
    creditcardid: Long,
    creditcardapprovalcode: String,
    currencyrateid: Long,
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