package adventureworks.layers.workload.domain.dimension

import java.sql.Timestamp
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._

// This case class shouldn't be used as encoder
final case class Product(
    productid: Long,
    name: String,
    productnumber: String,
    makeflag: Boolean,
    finishedgoodsflag: Boolean,
    color: String,
    safetystocklever: Short,
    reorderpoint: Short,
    standardcost: BigDecimal,
    listprice: BigDecimal,
    size: String,
    sizeunitmeasurecode: String,
    weightunitmeasurecode: String,
    weight: BigDecimal,
    daystomanufacture: Integer,
    productLine: String,
    // spark fails when case class has field `class`
    `class`: String,
    style: String,
    productsubcategoryid: Long,
    productmodelid: Long,
    sellstartdate: Timestamp,
    sellenddate: Timestamp,
    discontinueddate: Timestamp,
    rowguid: String,
    modifieddate: Timestamp
)

object Product {
    def schema: StructType = {
        // Encoders.product[Product].schema

        /* It's necessary to use a StructType to avoid problems with
        the *class* field */
        StructType(Array(
            StructField("productid", IntegerType, true),
            StructField("name", StringType, true),
            StructField("productnumber", StringType, true),
            StructField("makeflag", BooleanType, true),
            StructField("finishedgoodsflag", BooleanType, true),
            StructField("color", StringType, true),
            StructField("safetystocklevel", ShortType, true),
            StructField("reorderpoint", ShortType, true),
            StructField("standardcost", DecimalType(38,18), true),
            StructField("listprice", DecimalType(38,18), true),
            StructField("size", StringType, true),
            StructField("sizeunitmeasurecode", StringType, true),
            StructField("weightunitmeasurecode", StringType, true),
            StructField("weight", DecimalType(8,2), true),
            StructField("daystomanufacture", IntegerType, true),
            StructField("productline", StringType, true),
            StructField("class", StringType, true),
            StructField("style", StringType, true),
            StructField("productsubcategoryid", IntegerType, true),
            StructField("productmodelid", IntegerType, true),
            StructField("sellstartdate", TimestampType, true),
            StructField("sellenddate", TimestampType, true),
            StructField("discontinueddate", TimestampType, true),
            StructField("rowguid", StringType, true),
            StructField("modifieddate", TimestampType, true),
        ))
    }
}
