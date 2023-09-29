import Dependencies.*
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"
ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / organizationName := "The National Archives"

lazy val root = (project in file("."))
  .settings(
    name := "dr2-entity-event-generator",
    libraryDependencies ++= Seq(
      awsDynamoDbClient,
      awsJavaEvents,
      awsLambdaCore,
      awsLambda,
      awsSnsClient,
      awsSecretsManager,
      catsEffect,
      log4jSlf4j,
      log4jCore,
      log4jTemplateJson,
      pureConfig,
      pureConfigCats,
      sttpClient,
      mockitoScala,
      preservicaClient,
      scalaTest % Test,
      typeSafeConfig,
      wiremock % Test
    )
  )
(assembly / assemblyJarName) := "entityEventGenerator.jar"

scalacOptions ++= Seq("-Wunused:imports", "-Werror", "-unchecked", "-deprecation")

(assembly / assemblyMergeStrategy) := {
  case PathList(ps@_*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
  case _ => MergeStrategy.first
}
