lazy val root = (project in file("."))
  .aggregate(ingestLambdasRoot)
  .settings(
    name := "preservation",
    scalaVersion := "3.4.2"
  )

lazy val ingestLambdasRoot = project in file("./scala/lambdas")

