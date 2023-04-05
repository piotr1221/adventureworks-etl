package sources

sealed trait DataLakeLayer { val path: String }
case object Base extends DataLakeLayer { val path = "F:/Projects/ETL/AdventureWorks/datalake" }
case object Workload extends DataLakeLayer { val path = s"${Base.path}/workload" }
case object Landing extends DataLakeLayer { val path = s"${Base.path}/landing" }
case object Curated extends DataLakeLayer { val path = s"${Base.path}/curated" }
case object Functional extends DataLakeLayer { val path = s"${Base.path}/functional" }