import Dependencies.*
ThisBuild / scalaVersion := "3.5.1"

lazy val root = (project in file("."))
  .aggregate(ingestLambdasRoot)
  .settings(
    name := "ingest",
    scalaVersion := "3.5.1"
  )

lazy val ingestLambdasRoot = project in file("./scala/lambdas")

lazy val e2eTests = (project in file("./scala/e2e-tests"))
  .settings(
    publish / skip := true,
    libraryDependencies ++= Seq(
      pureConfig % Test,
      fs2Core % Test,
      s3Client % Test,
      sqsClient % Test,
      scalaTest % Test,
    ),
  )

