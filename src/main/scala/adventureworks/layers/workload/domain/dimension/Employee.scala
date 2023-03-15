package adventureworks.layers.workload.domain.dimension

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoders

final case class Employee(
    businessentityid: Option[Int],
    nationalidnumber: String,
    loginid: String,
    jobtitle: String,
    birthdate: Date,
    maritalstatus: String,
    gender: String,
    hiredate: Date,
    salariedflag: Option[Boolean],
    vacationhours: Option[Short],
    sickleavehours: Option[Short],
    currentflag: Option[Boolean],
    rowguid: String,
    modifieddate: Timestamp,
    organizationnode: String
)

object Employee {
    def schema: StructType = {
        Encoders.product[Employee].schema
    }
}
