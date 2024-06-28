package uk.gov.nationalarchives.ingestupsertarchivefolders

import cats.effect.IO
import uk.gov.nationalarchives.{DADynamoDBClient, DAEventBridgeClient}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.ingestupsertarchivefolders.Lambda.{Config, Dependencies, StepFnInput}

import cats.effect.unsafe.implicits.global

object LambdaRunner extends App {
  """
    |{
    |  "batchId": "TST-2024-CCTT",
    |  "s3Bucket": "intg-dr2-ingest-raw-cache",
    |  "s3Prefix": "TST-2024-CCTT/",
    |  "archiveHierarchyFolders": [
    |    "8f2fa5cb-24b8-4ede-a835-b9913408006c",
    |    "f93f69c1-d1e7-46f3-a39b-3f19e2ca144e",
    |    "fc1c0cab-aa3a-4e97-8bf2-0bc811999838"
    |  ],
    |  "contentFolders": [],
    |  "contentAssets": [
    |    "5511cf8b-947c-43c7-b62d-28785284c535"
    |  ]
    |}
    |""".stripMargin
  val input = StepFnInput(
    "TST-2024-CCTT",
    List("8f2fa5cb-24b8-4ede-a835-b9913408006c", "f93f69c1-d1e7-46f3-a39b-3f19e2ca144e", "fc1c0cab-aa3a-4e97-8bf2-0bc811999838"),
    Nil,
    List("5511cf8b-947c-43c7-b62d-28785284c535")
  )
  val config = Config("", "", "intg-dr2-ingest-files")
  val dependencies = Fs2Client
    .entityClient(config.apiUrl, config.secretName)
    .map { client =>
      Dependencies(client, DADynamoDBClient[IO](), DAEventBridgeClient[IO]())
    }
    .unsafeRunSync()
  new Lambda().handler(input, config, dependencies)
}
