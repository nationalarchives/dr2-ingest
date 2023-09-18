import Dependencies._
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
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
