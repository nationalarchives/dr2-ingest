import Dependencies._
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .aggregate(
    entityEventGenerator,
    getLatestPreservicaVersion,
    ingestAssetOpexCreator,
    ingestAssetReconciler,
    ingestFolderOpexCreator,
    ingestMapper,
    ingestParentFolderOpexCreator,
    ingestParsedCourtDocumentEventHandler,
    ingestUpsertArchiveFolders,
    ingestWorkflowMonitor,
    preservicaConfig,
    startWorkflow
  )

lazy val commonSettings = Seq(
  name := baseDirectory.value.getName,
  libraryDependencies ++= Seq(
    circeCore,
    circeParser,
    circeGeneric,
    log4jSlf4j,
    log4jCore,
    log4jTemplateJson,
    log4CatsCore,
    log4CatsSlf4j,
    lambdaCore,
    lambdaJavaEvents,
    pureConfig,
    pureConfigCats,
    mockito % Test,
    mockitoScalaTest % Test,
    scalaTest % Test,
    wiremock % Test
  ),
  assembly / assemblyOutputPath := file(s"target/outputs/${name.value}"),
  (assembly / assemblyMergeStrategy) := {
    case PathList(ps @ _*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
    case _                                                   => MergeStrategy.first
  },
  scalacOptions ++= Seq("-Wunused:imports", "-Werror", "-deprecation"),
  (Test / fork) := true,
  (Test / envVars) := Map(
    "AWS_ACCESS_KEY_ID" -> "accesskey",
    "AWS_SECRET_ACCESS_KEY" -> "secret",
    "AWS_LAMBDA_FUNCTION_NAME" -> "test"
  )
)

lazy val ingestMapper = (project in file("ingest-mapper"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      awsCrt,
      fs2Reactive,
      s3Client,
      dynamoClient,
      scalaXml,
      sttpClientFs2,
      sttpUpickle,
      reactorTest % Test
    )
  )

lazy val ingestParentFolderOpexCreator = (project in file("ingest-parent-folder-opex-creator"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      s3Client,
      fs2Core,
      reactorTest % Test,
      scalaXml,
      upickle

    )
  )

lazy val ingestUpsertArchiveFolders = (project in file("ingest-upsert-archive-folders"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      eventBridgeClient,
      preservicaClient,
      pureConfig,
      dynamoFormatters

    )
  )

lazy val ingestWorkflowMonitor = (project in file("ingest-workflow-monitor"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies += preservicaClient
  )

lazy val preservicaConfig = (project in file("preservica-config"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      preservicaClient,
      s3Client,
      scalaXml,
      scalaParserCombinators,
      jaxb
    )
  )

lazy val ingestFolderOpexCreator = (project in file("ingest-folder-opex-creator"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      awsCrt,
      fs2Core,
      scalaXml,
      upickle,
      dynamoClient,
      dynamoFormatters,
      s3Client
    )
  )

lazy val startWorkflow = (project in file("ingest-start-workflow"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies += preservicaClient
  )

lazy val entityEventGenerator = (project in file("entity-event-generator-lambda"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      awsSecretsManager,
      catsEffect,
      dynamoClient,
      preservicaClient,
      snsClient,
      sttpClient,
      typeSafeConfig
    ),
  )

lazy val getLatestPreservicaVersion = (project in file("get-latest-preservica-version-lambda"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      dynamoFormatters,
      snsClient,
      preservicaClient
    )
  )

lazy val ingestAssetReconciler = (project in file("ingest-asset-reconciler"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      dynamoFormatters,
      preservicaClient
    )
  )
lazy val ingestAssetOpexCreator = (project in file("ingest-asset-opex-creator"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      awsCrt,
      fs2Core,
      dynamoClient,
      dynamoFormatters,
      scalaXml,
      s3Client,
      upickle
    )
  )

lazy val ingestParsedCourtDocumentEventHandler = (project in file("ingest-parsed-court-document-event-handler"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      awsCrt,
      commonsCompress,
      fs2IO,
      circeGenericExtras,
      s3Client,
      sfnClient,
      reactorTest % Test
    )
  )

  lazy val utils = (project in file("utils"))
    .settings(commonSettings)






