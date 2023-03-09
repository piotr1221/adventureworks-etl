val scala3Version = "2.12.15"

lazy val root = project
  .in(file("."))
  .settings(
    name := "AdventureWorksETL",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.2" % Provided,
      "org.apache.spark" %% "spark-sql" % "3.3.2" % Provided,
      "org.postgresql" % "postgresql" % "42.5.4"
      )
  )
