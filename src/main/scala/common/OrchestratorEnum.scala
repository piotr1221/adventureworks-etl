package common

//sealed trait OrchestratorEnum

//object OrchestratorEnum {
//case object Source extends OrchestratorEnum
//case object Cleaner extends OrchestratorEnum
//}

object OrchestratorEnum extends Enumeration {
  val Source, Cleaner = Value
}