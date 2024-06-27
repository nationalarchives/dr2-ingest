package uk.gov.nationalarchives.rotatepreservationsystempassword

import cats.effect.IO
import io.circe.{Decoder, HCursor}
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import software.amazon.awssdk.services.secretsmanager.model.DescribeSecretResponse
import uk.gov.nationalarchives.DASecretsManagerClient
import uk.gov.nationalarchives.DASecretsManagerClient.Stage
import uk.gov.nationalarchives.DASecretsManagerClient.Stage.*
import uk.gov.nationalarchives.dp.client.UserClient
import uk.gov.nationalarchives.dp.client.UserClient.ResetPasswordRequest
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client.userClient
import uk.gov.nationalarchives.rotatepreservationsystempassword.Lambda.{*, given}
import uk.gov.nationalarchives.rotatepreservationsystempassword.Lambda.RotationStep.*
import uk.gov.nationalarchives.utils.LambdaRunner

import scala.jdk.CollectionConverters.*

class Lambda extends LambdaRunner[RotationEvent, Unit, Config, Dependencies] {

  override def dependencies(config: Config): IO[Dependencies] = userClient(config.apiUrl, config.secretName).map { userClient =>
    Dependencies(userClient, secretId => DASecretsManagerClient[IO](secretId))
  }

  override def handler: (RotationEvent, Config, Dependencies) => IO[Unit] = (event, config, dependencies) =>
    for {
      client <- IO.pure(dependencies.secretsManagerClient(event.secretId))
      token = event.clientRequestToken
      secretId = event.secretId
      describeSecretResponse <- client.describeSecret()
      versions <- versions(describeSecretResponse)
      _ <- IO.raiseWhen(!describeSecretResponse.rotationEnabled())(new Exception(s"Secret $secretId is not enabled for rotation."))
      _ <- IO.raiseWhen(!versions.contains(event.clientRequestToken))(new Exception(s"Secret version $token has no stage set for rotation of secret $secretId."))
      _ <- IO.raiseWhen(versions(token).contains(Current))(new Exception(s"Secret $secretId is already at AWSCURRENT."))
      _ <- IO.raiseWhen(!versions(token).contains(Pending))(new Exception(s"Secret version $token not set as AWSPENDING for rotation of secret $secretId."))
      _ <- event.step match
        case CreateSecret => createSecret(client, token, config.apiUser)
        case SetSecret    => setSecret(client, dependencies.userClient, config.apiUser)
        case TestSecret   => dependencies.userClient.testNewPassword()
        case FinishSecret => finishSecret(client, token)
    } yield ()

  private def finishSecret(client: DASecretsManagerClient[IO], token: String) = for {
    describeSecretResponse <- client.describeSecret()
    versions <- versions(describeSecretResponse)
    currentVersions <- IO {
      versions.collect {
        case (version, stages) if stages.headOption.contains(Current) => version
      }
    }
    _ <- client.updateSecretVersionStage(token, currentVersions.head)
  } yield ()

  private def createSecret(client: DASecretsManagerClient[IO], token: String, apiUser: String) = for {
    _ <- client.getSecretValue[Map[String, String]]()
    _ <- client.getSecretValue[Map[String, String]](token, Pending).recoverWith { _ =>
      for {
        password <- client.generateRandomPassword()
        _ <- client.putSecretValue(Map(apiUser -> password), Pending, Option(token))
      } yield ()
    }
  } yield ()

  private def setSecret(client: DASecretsManagerClient[IO], userClient: UserClient[IO], apiUser: String) = for {
    currentSecret <- client.getSecretValue[Map[String, String]]()
    newSecret <- client.getSecretValue[Map[String, String]](Pending)
    resetPasswordRequest <- resetPasswordRequest(apiUser, currentSecret, newSecret)
    _ <- userClient.resetPassword(resetPasswordRequest)
  } yield ()

  private def getPassword(secret: Map[String, String], apiUser: String, stage: Stage) =
    IO.fromOption(secret.get(apiUser))(new Exception(s"No $stage secret found for user $apiUser"))

  private def resetPasswordRequest(apiUser: String, currentSecret: Map[String, String], newSecret: Map[String, String]) = for {
    currentPassword <- getPassword(currentSecret, apiUser, Current)
    newPassword <- getPassword(newSecret, apiUser, Pending)
  } yield ResetPasswordRequest(currentPassword, newPassword)

  private def versions(describeSecretResponse: DescribeSecretResponse): IO[Map[String, List[Stage]]] = IO {
    describeSecretResponse.versionIdsToStages.asScala.view
      .mapValues(
        _.asScala
          .map {
            case "AWSCURRENT"  => Current
            case "AWSPENDING"  => Pending
            case "AWSPREVIOUS" => Previous
          }
          .toList
      )
      .toMap
  }
}

object Lambda:

  enum RotationStep:
    case CreateSecret, SetSecret, TestSecret, FinishSecret

  given Decoder[RotationEvent] = (c: HCursor) =>
    for {
      step <- c.downField("Step").as[String]
      secretId <- c.downField("SecretId").as[String]
      clientRequestToken <- c.downField("ClientRequestToken").as[String]
    } yield RotationEvent(RotationStep.valueOf(step.capitalize), secretId, clientRequestToken)

  case class RotationEvent(step: RotationStep, secretId: String, clientRequestToken: String)

  case class Config(apiUrl: String, secretName: String, apiUser: String) derives ConfigReader

  case class Dependencies(userClient: UserClient[IO], secretsManagerClient: String => DASecretsManagerClient[IO])
