import Dependencies.*
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

ThisBuild / organization := "uk.gov.nationalarchives"
name := "lambdas"

lazy val ingestLambdasRoot = (project in file("."))
  .aggregate(
    dynamoFormatters.jvm,
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
    preservicaConfig,
    rotatePreservationSystemPassword,
    startWorkflow
  )

lazy val commonSettings = Seq(
  name := baseDirectory.value.getName,
  libraryDependencies ++= Seq(
    "org.scala-js"       % "sbt-scalajs"                   % "1.16.0",
    circeCore,
    circeParser,
    circeGeneric,
    feral,
    log4jSlf4j,
    log4jCore,
    log4jTemplateJson,
    log4CatsCore,
    log4CatsSlf4j,
    lambdaCore,
    lambdaJavaEvents,
    pureConfig,
    pureConfigGeneric,
    mockito % Test,
    scalaTest % Test,
    wiremock % Test
  ),
  assembly / assemblyOutputPath := file(s"target/outputs/${name.value}"),
  (assembly / assemblyMergeStrategy) := {
    case PathList(ps @ _*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
    case _                                                   => MergeStrategy.first
  },
  scalacOptions ++= Seq("-deprecation", "-feature", "-language:implicitConversions"),
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
      s3Client.value,
      dynamoClient.value,
      scalaXml.value,
      sttpClientFs2,
      sttpCirce,
      upickle,
      reactorTest % Test
    )
  )

lazy val ingestFilesChangeHandler = (project in file("ingest-files-change-handler"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters.jvm)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient.value,
      snsClient
    )
  )

lazy val ingestParentFolderOpexCreator = (project in file("ingest-parent-folder-opex-creator"))
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      s3Client.value,
      fs2Core,
      reactorTest % Test,
      scalaXml.value
    )
  )

lazy val ingestUpsertArchiveFolders = (project in file("ingest-upsert-archive-folders"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters.jvm)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient.value,
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
      s3Client.value,
      scalaXml.value,
      scalaParserCombinators,
      jaxb
    )
  )

lazy val ingestFolderOpexCreator = (project in file("ingest-folder-opex-creator"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters.jvm)
  .settings(
    libraryDependencies ++= Seq(
      awsCrt,
      fs2Core,
      preservicaClient,
      scalaXml.value,
      dynamoClient.value,
      s3Client.value
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
      dynamoClient.value,
      preservicaClient,
      snsClient,
      sttpClient,
      typeSafeConfig
    )
  )

lazy val getLatestPreservicaVersion = (project in file("get-latest-preservica-version-lambda"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters.jvm)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient.value,
      snsClient,
      preservicaClient
    )
  )

lazy val ingestFindExistingAsset = (project in file("ingest-find-existing-asset"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters.jvm)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient.value,
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
  .dependsOn(utils, dynamoFormatters.jvm)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient.value,
      preservicaClient
    )
  )

lazy val ingestAssetOpexCreator = (project in file("ingest-asset-opex-creator"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters.js)
  .settings(
    libraryDependencies ++= Seq(
      awsCrt,
      "co.fs2" %%% "fs2-core" % fs2Version,
      dynamoClient.value,
      preservicaClient,
      scalaXml.value,
      s3Client.value
    )
  ).enablePlugins(LambdaJSPlugin)

lazy val ingestParsedCourtDocumentEventHandler = (project in file("ingest-parsed-court-document-event-handler"))
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters.jvm)
  .settings(
    libraryDependencies ++= Seq(
      awsCrt,
      commonsCompress,
      dynamoClient.value,
      fs2IO,
      s3Client.value,
      sfnClient,
      reactorTest % Test
    )
  )

lazy val utils = (project in file("utils"))
  .settings(commonSettings)
  .settings(
    libraryDependencies += dynosaur.value
  )

lazy val dynamoFormatters = (crossProject(JSPlatform, JVMPlatform) in file("dynamo-formatters"))
  .settings(
    libraryDependencies ++= Seq(
      awsDynamoDb,
      dynosaur.value,
      scalaTest % Test
    )
  )
  .disablePlugins(AssemblyPlugin)
