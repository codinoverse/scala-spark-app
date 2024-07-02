ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "scala-spark-app",
    idePackagePrefix := Some("com.codinoverse")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.1",
  "org.apache.logging.log4j" % "log4j-api" % "2.17.1",
    "org.scalatest" %% "scalatest" % "3.0.8" % Test

)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _ @_*) => MergeStrategy.discard
  case _                           => MergeStrategy.last
}
run / compile in Compile := (run / compile in Compile).dependsOn(compile in Test).value
run / mainClass := Some("CodinoverseCatalogueApp")

