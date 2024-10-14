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
      log4CatsCore % Test,
      log4CatsSlf4j % Test,
      dynamoClient % Test,
      log4jSlf4j % Test,
      log4jCore % Test,
      log4jTemplateJson % Test,
      pureConfig % Test,
      fs2Core % Test,
      s3Client % Test,
      sfnClient % Test,
      sqsClient % Test,
      scalaTest % Test,
    ),
  )

