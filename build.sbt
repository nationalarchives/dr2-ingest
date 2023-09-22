import Dependencies._
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file(".")).settings(
  name := "dr2-ingest-mapper",
  libraryDependencies ++= Seq(
    fs2Csv,
    fs2CsvGeneric,
    fs2Reactive,
    log4jSlf4j,
    log4jCore,
    log4jTemplateJson,
    lambdaCore,
    pureConfig,
    pureConfigCats,
    s3Client,
    dynamoClient,
    scalaXml,
    sttp,
    sttpUpickle,
    "io.projectreactor" % "reactor-test" % "3.5.10" % Test,
    scalaTest % Test,
    mockito % Test,
    wiremock % Test
  )
)
(assembly / assemblyJarName) := "dr2-ingest-mapper.jar"

scalacOptions ++= Seq("-Wunused:imports", "-Werror")
ThisBuild / scalacOptions ++= Seq("-unchecked", "-deprecation")

(assembly / assemblyMergeStrategy) := {
  case PathList(ps @ _*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
  case _                                                   => MergeStrategy.first
}
