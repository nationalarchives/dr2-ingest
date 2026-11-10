package uk.gov.nationalarchives.rotatepreservationsystempassword

import cats.effect.IO
import cats.syntax.all.*
import io.circe.{Decoder, Encoder, HCursor, Json}
import pureconfig.ConfigReader
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
import scala.concurrent.duration.*

class Lambda extends LambdaRunner[RotationEvent, Unit, Config, Dependencies] {

  given Decoder[AuthDetails] = (c: HCursor) =>
    for {
      userName <- c.downField("userName").as[String]
      password <- c.downField("password").as[String]
      apiUrl <- c.downField("apiUrl").as[String]
    } yield AuthDetails(userName, password, apiUrl)

  given Encoder[AuthDetails] = (authDetails: AuthDetails) =>
    Json.obj("userName" -> Json.fromString(authDetails.userName), "password" -> Json.fromString(authDetails.password), "apiUrl" -> Json.fromString(authDetails.apiUrl))

  override def dependencies(config: Config): IO[Dependencies] =
    IO(Dependencies(secretId => userClient(secretId), secretId => DASecretsManagerClient[IO](secretId = secretId)))

  override def handler: (RotationEvent, Config, Dependencies) => IO[Unit] = (event, config, dependencies) => {
    val secretsClient = dependencies.secretsManagerClient(event.secretId)

    def createPassword(): IO[String] =
      secretsClient.generateRandomPassword().flatMap { newPassword =>
        if newPassword.validPassword then IO.pure(newPassword) else IO.sleep(500.milliseconds) >> createPassword()
      }

    def getAuthDetailsFromSecret(stage: Stage) =
      secretsClient.getSecretValue[AuthDetails](stage)

    def finishSecret(token: String) = for {
      currentVersions <- getSecretVersions(Current)
      _ <- secretsClient.updateSecretVersionStage(token.some, currentVersions.headOption)
    } yield ()

    def createSecret(token: String) = for {
      currentSecret <- getAuthDetailsFromSecret(Current)
      _ <- secretsClient.getSecretValue[AuthDetails](token, Pending).recoverWith { _ =>
        for {
          password <- createPassword()
          _ <- secretsClient.putSecretValue(currentSecret.copy(password = password), Pending, Option(token))
        } yield ()
      }
    } yield ()

    def setSecret(userClient: UserClient[IO]) = for {
      currentSecret <- getAuthDetailsFromSecret(Current)
      newSecret <- getAuthDetailsFromSecret(Pending)
      _ <- userClient.resetPassword(ResetPasswordRequest(currentSecret.password, newSecret.password)).onError(removePendingSecret())
    } yield ()

    def getSecretVersions(stage: Stage) = for {
      describeSecretResponse <- secretsClient.describeSecret()
      versions <- versions(describeSecretResponse)
      stageVersions <- IO {
        versions.collect {
          case (version, stages) if stages.headOption.contains(stage) => version
        }
      }
    } yield stageVersions

    def removePendingSecret(): PartialFunction[Throwable, IO[Unit]] =
      case err =>
        for {
          _ <- logger.error(err)("Error setting the secret in preservation system; removing 'Pending' secret")
          pendingVersions <- getSecretVersions(Pending)
          _ <- secretsClient.updateSecretVersionStage(None, pendingVersions.headOption, Pending)
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

  enum RotationStep:
    case CreateSecret, SetSecret, TestSecret, FinishSecret

  given Decoder[RotationEvent] = (c: HCursor) =>
    for {
      step <- c.downField("Step").as[String]
      secretId <- c.downField("SecretId").as[String]
      clientRequestToken <- c.downField("ClientRequestToken").as[String]
    } yield RotationEvent(RotationStep.valueOf(step.capitalize), secretId, clientRequestToken)

  case class RotationEvent(step: RotationStep, secretId: String, clientRequestToken: String)

  case class Config() derives ConfigReader

  case class Dependencies(userClient: String => IO[UserClient[IO]], secretsManagerClient: String => DASecretsManagerClient[IO])

  case class AuthDetails(userName: String, password: String, apiUrl: String)

  extension (s: String)
    private def containsCharMix = List(s.exists(_.isUpper), s.exists(_.isLower), s.exists(_.isDigit), s.exists(!_.isLetterOrDigit)).count(identity) >= 3
    private def repeating = """(.)\1\1""".r.findFirstIn(s.toLowerCase).nonEmpty
    private def validPassword = s.length >= 8 && s.length <= 64 && !repeating && containsCharMix
