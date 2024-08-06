ThisBuild / scalaVersion := "3.4.2"

lazy val root = (project in file("."))
  .aggregate(ingestLambdasRoot)
  .settings(
    name := "preservation"
  )

lazy val ingestLambdasRoot = project in file("./scala/lambdas")

