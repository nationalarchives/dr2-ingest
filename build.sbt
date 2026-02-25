import Dependencies.*
ThisBuild / scalaVersion := "3.8.2"

lazy val root = (project in file("."))
  .aggregate(ingestLambdasRoot)
  .settings(
    name := "ingest",
    scalaVersion := "3.8.2"
  )

lazy val ingestLambdasRoot = project in file("./scala/lambdas")

lazy val testsSettings = Seq(
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
    fs2Reactive % Test,
    s3Client % Test,
    sfnClient % Test,
    sqsClient % Test,
    scalaTest % Test
  )
)
