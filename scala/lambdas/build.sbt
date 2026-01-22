import Dependencies.*
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

import java.nio.file.{Files, StandardCopyOption}

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
    ingestFailureNotifications,
    ingestFindExistingAsset,
    ingestFlowControl,
    ingestFolderOpexCreator,
    ingestMapper,
    ingestParentFolderOpexCreator,
    ingestParsedCourtDocumentEventHandler,
    ingestUpsertArchiveFolders,
    ingestValidateGenericIngestInputs,
    ingestWorkflowMonitor,
    preingestPaImporter,
    postIngestStateChangeHandler,
    postingestMessageResender,
    preingestCourtDocImporter,
    preingestCourtDocPackageBuilder,
    preingestTdrAggregator,
    preingestDriAggregator,
    preingestAdHocAggregator,
    preingestPaAggregator,
    preingestCourtDocumentAggregator,
    preIngestTdrPackageBuilder,
    preIngestPaPackageBuilder,
    preingestDriPackageBuilder,
    preingestAdHocPackageBuilder,
    rotatePreservationSystemPassword,
    startWorkflow
  )

lazy val commonSettings = Seq(
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
  dependencyOverrides ++= Seq(awsDynamo, commonsLogging),
  assembly / assemblyOutputPath := file(s"target/outputs/${name.value}"),
  (assembly / assemblyMergeStrategy) := {
    case PathList(ps @ _*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
    case _                                                   => MergeStrategy.first
  },
  scalacOptions ++= Seq("-Yretain-trees", "-Xmax-inlines", "40", "-Wunused:imports", "-Werror", "-deprecation", "-feature", "-language:implicitConversions"),
  (Test / fork) := true,
  (Test / envVars) := Map(
    "AWS_ACCESS_KEY_ID" -> "accesskey",
    "AWS_SECRET_ACCESS_KEY" -> "secret",
    "AWS_LAMBDA_FUNCTION_NAME" -> "test"
  )
)

lazy val copySchema = taskKey[Unit]("Copies the PA json schema file to the resources directory")

lazy val preingestCourtDocImporter = (project in file("preingest-courtdoc-importer"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      awsCrt,
      commonsCompress,
      fs2IO,
      s3Client,
      sqsClient,
      reactorTest % Test
    ),
    dependencyOverrides += commonsLang
  )

lazy val preingestPaImporter = (project in file("preingest-pa-importer"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    copySchema := {
      val schemaLocation = baseDirectory.value / "../../../" / "common" / "preingest-pa" / "metadata-schema.json"
      Files.copy(schemaLocation.toPath, (Compile / resourceDirectory).value.toPath.resolve("metadata-schema.json"), StandardCopyOption.REPLACE_EXISTING)
    },
    libraryDependencies ++= Seq(
      fs2Core,
      fs2Reactive,
      jsonSchemaValidator,
      s3Client,
      sqsClient,
      reactorTest % Test
    ),
    Compile / compile := (Compile / compile).dependsOn(copySchema).value,
    Test / compile := (Test / compile).dependsOn(copySchema).value
  )

lazy val ingestMapper = (project in file("ingest-mapper"))
  .settings(name := baseDirectory.value.getName)
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

lazy val ingestFlowControl = (project in file("ingest-flow-control"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      sfnClient,
      ssmClient
    )
  )

lazy val ingestParentFolderOpexCreator = (project in file("ingest-parent-folder-opex-creator"))
  .settings(name := baseDirectory.value.getName)
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
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      preservicaClient,
      pureConfig
    )
  )

lazy val ingestWorkflowMonitor = (project in file("ingest-workflow-monitor"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies += preservicaClient
  )

lazy val ingestFolderOpexCreator = (project in file("ingest-folder-opex-creator"))
  .settings(name := baseDirectory.value.getName)
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
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies += preservicaClient
  )

lazy val entityEventGenerator = (project in file("entity-event-generator-lambda"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      catsEffect,
      dynamoClient,
      preservicaClient,
      snsClient,
      sttpClient,
      typeSafeConfig
    )
  )

lazy val getLatestPreservicaVersion = (project in file("get-latest-preservica-version-lambda"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      eventBridgeClient,
      preservicaClient
    )
  )

lazy val ingestFindExistingAsset = (project in file("ingest-find-existing-asset"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      preservicaClient
    )
  )

lazy val rotatePreservationSystemPassword = (project in file("rotate-preservation-system-password"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      preservicaClient,
      secretsManagerClient
    )
  )

lazy val ingestAssetReconciler = (project in file("ingest-asset-reconciler"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      preservicaClient
    )
  )

lazy val ingestAssetOpexCreator = (project in file("ingest-asset-opex-creator"))
  .settings(name := baseDirectory.value.getName)
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
  .settings(name := baseDirectory.value.getName)
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
    ),
    dependencyOverrides += commonsLang
  )

lazy val ingestValidateGenericIngestInputs = (project in file("ingest-validate-generic-ingest-inputs"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      awsCrt,
      catsEffect,
      circeFs2,
      fs2IO,
      fs2Reactive,
      jawnFs2,
      jsonSchemaValidator,
      s3Client,
      sfnClient,
      sttpClientFs2,
      sttpCirce,
      upickle,
      reactorTest % Test
    )
  )

lazy val postIngestStateChangeHandler = (project in file("postingest-state-change-handler"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      catsEffect,
      circeFs2,
      dynamoClient,
      fs2Reactive,
      jsonSchemaValidator % Test,
      snsClient,
      sqsClient,
      reactorTest % Test,
      scalaCheck % Test,
      scalaCheckPlus % Test
    )
  )

lazy val packageBuilderSettings = libraryDependencies ++= Seq(
  circeFs2,
  dynamoClient,
  fs2Reactive,
  jsonSchemaValidator % Test,
  s3Client,
  reactorTest % Test,
  scalaCheck % Test,
  scalaCheckPlus % Test
)

lazy val preIngestTdrPackageBuilder = (project in file("preingest-tdr-package-builder"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(packageBuilderSettings)

lazy val preingestCourtDocPackageBuilder = (project in file("preingest-courtdoc-package-builder"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      awsCrt,
      dynamoClient,
      fs2Reactive,
      fs2IO,
      s3Client,
      reactorTest % Test
    ),
    dependencyOverrides += commonsLang
  )

lazy val preingestDriPackageBuilder = (project in file("preingest-tdr-package-builder"))
  .settings(
    name := "preingest-dri-package-builder",
    target := (preIngestTdrPackageBuilder / baseDirectory).value / "target" / "preingest-dri-package-builder"
  )
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(packageBuilderSettings)

lazy val preingestAdHocPackageBuilder = (project in file("preingest-tdr-package-builder"))
  .settings(
    name := "preingest-adhoc-package-builder",
    target := (preIngestTdrPackageBuilder / baseDirectory).value / "target" / "preingest-adhoc-package-builder"
  )
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(packageBuilderSettings)

lazy val preIngestPaPackageBuilder = (project in file("preingest-tdr-package-builder"))
  .settings(
    name := "preingest-pa-package-builder",
    target := (preIngestTdrPackageBuilder / baseDirectory).value / "target" / "preingest-pa-package-builder"
  )
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(packageBuilderSettings)

lazy val aggregatorSettings = libraryDependencies ++= Seq(
  dynamoClient,
  sfnClient
)

lazy val preingestTdrAggregator = (project in file("preingest-tdr-aggregator"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(aggregatorSettings)

lazy val preingestDriAggregator = (project in file("preingest-tdr-aggregator"))
  .settings(
    name := "preingest-dri-aggregator",
    target := (preingestTdrAggregator / baseDirectory).value / "target" / "preingest-dri-aggregator"
  )
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(aggregatorSettings)

lazy val preingestAdHocAggregator = (project in file("preingest-tdr-aggregator"))
  .settings(
    name := "preingest-adhoc-aggregator",
    target := (preingestTdrAggregator / baseDirectory).value / "target" / "preingest-adhoc-aggregator"
  )
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(aggregatorSettings)

lazy val preingestPaAggregator = (project in file("preingest-tdr-aggregator"))
  .settings(
    name := "preingest-pa-aggregator",
    target := (preingestTdrAggregator / baseDirectory).value / "target" / "preingest-pa-aggregator"
  )
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(aggregatorSettings)

lazy val preingestCourtDocumentAggregator = (project in file("preingest-tdr-aggregator"))
  .settings(
    name := "preingest-courtdoc-aggregator",
    target := (preingestTdrAggregator / baseDirectory).value / "target" / "preingest-courtdoc-aggregator"
  )
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(aggregatorSettings)

lazy val custodialCopyQueueCreator = (project in file("custodial-copy-queue-creator"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils)
  .settings(
    libraryDependencies ++= Seq(
      preservicaClient,
      sqsClient
    )
  )

lazy val ingestFailureNotifications = (project in file("ingest-failure-notifications"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      snsClient,
      dynamoClient
    )
  )

lazy val postingestMessageResender = (project in file("postingest-message-resender"))
  .settings(name := baseDirectory.value.getName)
  .settings(commonSettings)
  .dependsOn(utils, dynamoFormatters)
  .settings(
    libraryDependencies ++= Seq(
      dynamoClient,
      sqsClient
    )
  )

lazy val utils = (project in file("utils"))
  .settings(commonSettings)
  .dependsOn(dynamoFormatters)
  .settings(
    libraryDependencies += scanamo,
    dependencyOverrides += awsDynamo
  )

lazy val dynamoFormatters = (project in file("dynamo-formatters"))
  .settings(
    libraryDependencies ++= Seq(
      scanamo,
      scalaTest % Test
    ),
    dependencyOverrides += awsDynamo
  )
  .disablePlugins(AssemblyPlugin)
