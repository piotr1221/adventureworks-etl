package adventureworks.layers.workload.domain.dimension

import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class Person(
    businessentityid: Option[Int],
    persontype: String,
    namestyle: Option[Boolean],
    title: String,
    firstname: String,
    middlename: String,
    lastname: String,
    suffix: String,
    emailpromotion: Option[Int],
    additionalcontactinfo: String,
    demographics: String,
    rowguid: String,
    modifieddate: Timestamp
)

object Person {
    def schema: StructType = {
        Encoders.product[Person].schema
    }
}