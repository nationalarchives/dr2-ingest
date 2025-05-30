package uk.gov.nationalarchives.rotatepreservationsystempassword

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import io.circe.{Decoder, Encoder}
import software.amazon.awssdk.services.secretsmanager.model.{DescribeSecretResponse, PutSecretValueResponse, UpdateSecretVersionStageResponse}
import uk.gov.nationalarchives.DASecretsManagerClient.Stage
import uk.gov.nationalarchives.DASecretsManagerClient
import uk.gov.nationalarchives.dp.client.UserClient
import uk.gov.nationalarchives.rotatepreservationsystempassword.Lambda.{Config, Dependencies, RotationEvent}

import scala.util.Random
import scala.jdk.CollectionConverters.*

object TestUtils:
  case class SecretStage(value: Map[String, String], stage: Stage)
  case class Secret(versionToStage: Map[String, List[SecretStage]], rotationEnabled: Boolean = true)

  case class Credentials(oldPassword: String, newPassword: String, testSuccess: Boolean = true)

  case class Errors(getSecret: Boolean = false)

  extension (errors: Option[Errors]) def raise(fn: Errors => Boolean, errorMessage: String): IO[Unit] = IO.raiseWhen(errors.exists(fn))(new Exception(errorMessage))

  def secretsManagerClient(ref: Ref[IO, Secret], errors: Option[Errors]): DASecretsManagerClient[IO] = new DASecretsManagerClient[IO]:
    override def generateRandomPassword(passwordLength: Int, excludeCharacters: String): IO[String] =
      IO.pure(Random.alphanumeric.filterNot(excludeCharacters.contains).slice(0, passwordLength).mkString)

    override def describeSecret(): IO[DescribeSecretResponse] = ref.get.map { existing =>
      val versionIdToStage = existing.versionToStage.map { case (versionId, stages) =>
        versionId -> stages.map(_.stage.toString).asJava
      }.asJava

      DescribeSecretResponse.builder
        .rotationEnabled(existing.rotationEnabled)
        .versionIdsToStages(versionIdToStage)
        .build()
    }

    override def getSecretValue[T](stage: Stage)(using decoder: Decoder[T]): IO[T] = errors.raise(_.getSecret, "Error getting secret") >>
      (for {
        existing <- ref.get
        versionMap <- IO.fromOption {
          existing.versionToStage.values.flatten.find(_.stage == stage).map(_.value)
        }(new Exception(s"Stage $stage not found"))
        _ <- IO.raiseWhen(versionMap.isEmpty)(new Exception(s"Secret not found for stage $stage"))
      } yield versionMap.asInstanceOf[T])

    override def getSecretValue[T](versionId: String, stage: Stage)(using decoder: Decoder[T]): IO[T] = ref.get.flatMap { existing =>
      for {
        existing <- ref.get
        versionMap <- IO.fromOption {
          existing.versionToStage(versionId).find(_.stage == stage).map(_.value)
        }(new Exception(s"Stage $stage not found"))
        _ <- IO.raiseWhen(versionMap.isEmpty)(new Exception(s"Secret not found for stage $stage"))
      } yield versionMap.asInstanceOf[T]
    }

    override def putSecretValue[T](secret: T, stage: Stage, clientRequestToken: Option[String])(using encoder: Encoder[T]): IO[PutSecretValueResponse] = ref
      .update { existing =>
        val secretStage = SecretStage(secret.asInstanceOf[Map[String, String]], stage)
        val newVersionStage: Map[String, List[SecretStage]] = existing.versionToStage + (clientRequestToken.getOrElse("") -> List(secretStage))
        existing.copy(versionToStage = newVersionStage)
      }
      .map(_ => PutSecretValueResponse.builder.build)

    override def updateSecretVersionStage(moveToVersionId: String, removeFromVersionId: String, stage: Stage): IO[UpdateSecretVersionStageResponse] = ref
      .update { existing =>
        val stageToMove = existing.versionToStage.get(removeFromVersionId).flatMap(_.find(_.stage == stage)).get
        val oldStages = existing.versionToStage(removeFromVersionId).filter(_.stage != stage)
        val newStages = stageToMove :: existing.versionToStage.getOrElse(moveToVersionId, Nil)
        val updatedMap: Map[String, List[SecretStage]] = Map(moveToVersionId -> newStages, removeFromVersionId -> oldStages)
        existing.copy(versionToStage = existing.versionToStage ++ updatedMap)
      }
      .map(_ => UpdateSecretVersionStageResponse.builder.build)

  def userClient(ref: Ref[IO, Credentials]): UserClient[IO] = new UserClient[IO]:
    override def resetPassword(changePasswordRequest: UserClient.ResetPasswordRequest): IO[Unit] = ref.update { existing =>
      existing.copy(oldPassword = changePasswordRequest.password, newPassword = changePasswordRequest.newPassword)
    }

    override def testNewPassword(): IO[Unit] = ref.get.flatMap { existing =>
      IO.raiseWhen(!existing.testSuccess)(new Exception("Password test failed"))
    }

  def runLambda(event: RotationEvent, secret: Secret, credentials: Credentials, errors: Option[Errors] = None): (Secret, Credentials, Either[Throwable, Unit]) = {
    for {
      secretRef <- Ref.of[IO, Secret](secret)
      credentialsRef <- Ref.of[IO, Credentials](credentials)
      dependencies = Dependencies(_ => IO(userClient(credentialsRef)), _ => secretsManagerClient(secretRef, errors))
      res <- new Lambda().handler(event, Config(), dependencies).attempt
      secret <- secretRef.get
      credentials <- credentialsRef.get
    } yield (secret, credentials, res)
  }.unsafeRunSync()

end TestUtils
