import Dependencies.*
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

ThisBuild / organization := "uk.gov.nationalarchives"
name := "lambdas"

lazy val ingestLambdasRoot = (project in file("."))
  .aggregate(
    custodialCopyQueueCreator,
    dynamoFormatters,
    entityEventGenerator,
    getLatestPreservicaVersion,
    ingestAssetOpexCreator,
    ingestAssetReconciler,
    ingestFilesChangeHandler,
    ingestFindExistingAsset,
    ingestFolderOpexCreator,
    ingestMapper,
    ingestParentFolderOpexCreator,
    ingestParsedCourtDocumentEventHandler,
    ingestUpsertArchiveFolders,
    ingestWorkflowMonitor,
    preingestTdrAggregator,
    custodialCopyAggregator,
    preIngestTdrPackageBuilder,
    preservicaConfig,
    rotatePreservationSystemPassword,
    startWorkflow
  )

lazy val aggregatorSettings =
  Seq(
    libraryDependencies ++= Seq(
      dynamoClient,
      sfnClient,
      sqsClient
    )
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
    scalaTest % Test,
    wiremock % Test
  ),
  assembly / assemblyOutputPath := file(s"target/outputs/${name.value}"),
  (assembly / assemblyMergeStrategy) := {
    case PathList(ps @ _*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
    case _                                                   => MergeStrategy.first
  },
  scalacOptions ++= Seq("-Yretain-trees", "-Xmax-inlines", "33", "-Wunused:imports", "-Werror", "-deprecation", "-feature", "-language:implicitConversions"),
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
      sttpCirce,
      upickle,
      reactorTest % Test
    )
  )

lazy val ingestFilesChangeHandler = (project in file("ingest-files-change-handler"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      snsClient
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
      scalaXml
    )
  )

lazy val ingestUpsertArchiveFolders = (project in file("ingest-upsert-archive-folders"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      eventBridgeClient,
      preservicaClient,
      pureConfig
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
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      awsCrt,
      fs2Core,
      preservicaClient,
      scalaXml,
      dynamoClient,
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
    )
  )

lazy val getLatestPreservicaVersion = (project in file("get-latest-preservica-version-lambda"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      snsClient,
      preservicaClient
    )
  )

lazy val ingestFindExistingAsset = (project in file("ingest-find-existing-asset"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      preservicaClient
    )
  )

lazy val rotatePreservationSystemPassword = (project in file("rotate-preservation-system-password"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      preservicaClient,
      secretsManagerClient
    )
  )

lazy val ingestAssetReconciler = (project in file("ingest-asset-reconciler"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      preservicaClient
    )
  )

lazy val ingestAssetOpexCreator = (project in file("ingest-asset-opex-creator"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      awsCrt,
      fs2Core,
      dynamoClient,
      preservicaClient,
      scalaXml,
      s3Client
    )
  )

lazy val ingestParsedCourtDocumentEventHandler = (project in file("ingest-parsed-court-document-event-handler"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      awsCrt,
      commonsCompress,
      dynamoClient,
      fs2IO,
      s3Client,
      sfnClient,
      reactorTest % Test
    )
  )


lazy val preIngestTdrPackageBuilder = (project in file("preingest-tdr-package-builder"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      circeFs2,
      dynamoClient,
      fs2Reactive,
      s3Client,
      reactorTest % Test,
      scalaCheck % Test,
      scalaCheckPlus % Test
    )
  )

lazy val preingestTdrAggregator = (project in file("preingest-tdr-aggregator"))
  .settings(commonSettings)
  .settings(aggregatorSettings)
  .dependsOn(utils)


lazy val custodialCopyQueueCreator = (project in file("custodial-copy-queue-creator"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      preservicaClient,
      sqsClient
    )
  )

lazy val custodialCopyAggregator = (project in file("custodial-copy-aggregator"))
  .settings(aggregatorSettings)
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    target := baseDirectory.value / "target" /"custodial-copy-aggregator",
    assembly / assemblyOutputPath := file(s"target/outputs/custodial-copy-aggregator"),
    baseDirectory := (preingestTdrAggregator / baseDirectory).value
  )

lazy val utils = (project in file("utils"))
  .settings(commonSettings)
  .settings(
    libraryDependencies += scanamo
  )

lazy val dynamoFormatters = (project in file("dynamo-formatters"))
  .settings(
    libraryDependencies ++= Seq(
      scanamo,
      scalaTest % Test
    )
  )
  .disablePlugins(AssemblyPlugin)
