ThisBuild / scalaVersion := "3.5.0"

lazy val root = (project in file("."))
  .aggregate(ingestLambdasRoot)
  .settings(
    name := "ingest",
    scalaVersion := "3.5.0"
  )

lazy val ingestLambdasRoot = project in file("./scala/lambdas")
