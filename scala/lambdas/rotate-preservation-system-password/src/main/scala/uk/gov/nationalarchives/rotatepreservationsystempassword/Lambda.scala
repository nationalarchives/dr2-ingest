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

  override def dependencies(config: Config): IO[Dependencies] =
    IO(Dependencies(secretId => userClient(config.apiUrl, secretId), secretId => DASecretsManagerClient[IO](secretId = secretId)))

  override def handler: (RotationEvent, Config, Dependencies) => IO[Unit] = (event, config, dependencies) => {
    val secretsClient = dependencies.secretsManagerClient(event.secretId)

    def getAuthDetailsFromSecret(stage: Stage) = for {
      (username, password) <- secretsClient.getSecretValue[Map[String, String]](stage).map(_.head)
    } yield AuthDetails(username, password)

    def finishSecret(token: String) = for {
      describeSecretResponse <- secretsClient.describeSecret()
      versions <- versions(describeSecretResponse)
      currentVersions <- IO {
        versions.collect {
          case (version, stages) if stages.headOption.contains(Current) => version
        }
      }
      _ <- secretsClient.updateSecretVersionStage(token, currentVersions.head)
    } yield ()

    def createSecret(token: String) = for {
      currentSecret <- getAuthDetailsFromSecret(Current)
      _ <- secretsClient.getSecretValue[Map[String, String]](token, Pending).recoverWith { _ =>
        for {
          password <- secretsClient.generateRandomPassword()
          _ <- secretsClient.putSecretValue(Map(currentSecret.username -> password), Pending, Option(token))
        } yield ()
      }
    } yield ()

    def setSecret(userClient: UserClient[IO]) = for {
      currentSecret <- getAuthDetailsFromSecret(Current)
      newSecret <- getAuthDetailsFromSecret(Pending)
      _ <- userClient.resetPassword(ResetPasswordRequest(currentSecret.password, newSecret.password))
    } yield ()

    for {
      userClient <- dependencies.userClient(event.secretId)
      token = event.clientRequestToken
      secretId = event.secretId
      describeSecretResponse <- secretsClient.describeSecret()
      versions <- versions(describeSecretResponse)
      _ <- IO.raiseWhen(!describeSecretResponse.rotationEnabled())(new Exception(s"Secret $secretId is not enabled for rotation."))
      _ <- IO.raiseWhen(!versions.contains(event.clientRequestToken))(new Exception(s"Secret version $token has no stage set for rotation of secret $secretId."))
      _ <- IO.raiseWhen(versions(token).contains(Current))(new Exception(s"Secret $secretId is already at AWSCURRENT."))
      _ <- IO.raiseWhen(!versions(token).contains(Pending))(new Exception(s"Secret version $token not set as AWSPENDING for rotation of secret $secretId."))
      _ <- event.step match
        case CreateSecret => createSecret(token)
        case SetSecret    => setSecret(userClient)
        case TestSecret   => userClient.testNewPassword()
        case FinishSecret => finishSecret(token)
    } yield ()
  }

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

  private case class AuthDetails(username: String, password: String)

  enum RotationStep:
    case CreateSecret, SetSecret, TestSecret, FinishSecret

  given Decoder[RotationEvent] = (c: HCursor) =>
    for {
      step <- c.downField("Step").as[String]
      secretId <- c.downField("SecretId").as[String]
      clientRequestToken <- c.downField("ClientRequestToken").as[String]
    } yield RotationEvent(RotationStep.valueOf(step.capitalize), secretId, clientRequestToken)

  case class RotationEvent(step: RotationStep, secretId: String, clientRequestToken: String)

  case class Config(apiUrl: String) derives ConfigReader

  case class Dependencies(userClient: String => IO[UserClient[IO]], secretsManagerClient: String => DASecretsManagerClient[IO])
