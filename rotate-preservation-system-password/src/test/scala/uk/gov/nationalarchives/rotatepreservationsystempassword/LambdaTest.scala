package uk.gov.nationalarchives.rotatepreservationsystempassword

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec
import cats.effect.unsafe.implicits.global
import io.circe.{Decoder, Encoder}
import org.mockito.{ArgumentCaptor, ArgumentMatcher, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, argThat}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers.*
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.secretsmanager.model.{DescribeSecretResponse, PutSecretValueResponse, UpdateSecretVersionStageResponse}
import uk.gov.nationalarchives.DASecretsManagerClient
import uk.gov.nationalarchives.DASecretsManagerClient.Stage
import uk.gov.nationalarchives.DASecretsManagerClient.Stage.*
import uk.gov.nationalarchives.dp.client.UserClient
import uk.gov.nationalarchives.dp.client.UserClient.ResetPasswordRequest
import uk.gov.nationalarchives.rotatepreservationsystempassword.Lambda.*
import uk.gov.nationalarchives.rotatepreservationsystempassword.Lambda.RotationStep.*

import scala.jdk.CollectionConverters.*

class LambdaTest extends AnyFlatSpec with MockitoSugar with EitherValues {

  def mockSecretsManagerClient(idToStages: Map[String, List[String]] = Map("token" -> List("AWSPENDING")), rotationEnabled: Boolean = true): DASecretsManagerClient[IO] = {
    val validDescribeResponse = DescribeSecretResponse.builder
      .rotationEnabled(rotationEnabled)
      .versionIdsToStages(idToStages.view.mapValues(_.asJava).toMap.asJava)
      .build
    val secretsManagerClient = mock[DASecretsManagerClient[IO]]
    when(secretsManagerClient.describeSecret()).thenReturn(IO(validDescribeResponse))
    secretsManagerClient
  }

  "handler" should "fail if rotation is not enabled" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val config = Config("http://localhost")
    val userClient = mock[UserClient[IO]]
    val secretsManagerClient = mockSecretsManagerClient(rotationEnabled = false)
    when(secretsManagerClient.describeSecret()).thenReturn(IO(DescribeSecretResponse.builder.rotationEnabled(false).build))
    val dependencies = Dependencies(secretId => IO.pure(userClient), secretId => secretsManagerClient)

    val ex = new Lambda().handler(rotationEvent, config, dependencies).attempt.unsafeRunSync().left.value

    ex.getMessage should equal("Secret id is not enabled for rotation.")
  }

  "handler" should "fail if there are no stages for the secret version" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val config = Config("http://localhost")
    val userClient = mock[UserClient[IO]]
    val secretsManagerClient = mockSecretsManagerClient(Map("anotherToken" -> List("AWSCURRENT")))
    val dependencies = Dependencies(secretId => IO.pure(userClient), secretId => secretsManagerClient)

    val ex = new Lambda().handler(rotationEvent, config, dependencies).attempt.unsafeRunSync().left.value

    ex.getMessage should equal("Secret version token has no stage set for rotation of secret id.")
  }

  "handler" should "fail if the version is already current" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val config = Config("http://localhost")
    val userClient = mock[UserClient[IO]]
    val secretsManagerClient = mockSecretsManagerClient(Map("token" -> List("AWSCURRENT")))

    val dependencies = Dependencies(secretId => IO.pure(userClient), secretId => secretsManagerClient)

    val ex = new Lambda().handler(rotationEvent, config, dependencies).attempt.unsafeRunSync().left.value

    ex.getMessage should equal("Secret id is already at AWSCURRENT.")
  }

  "handler" should "fail if the version does not have a pending stage" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val config = Config("http://localhost")
    val userClient = mock[UserClient[IO]]
    val secretsManagerClient = mockSecretsManagerClient(Map("token" -> Nil))

    val dependencies = Dependencies(secretId => IO.pure(userClient), secretId => secretsManagerClient)

    val ex = new Lambda().handler(rotationEvent, config, dependencies).attempt.unsafeRunSync().left.value

    ex.getMessage should equal("Secret version token not set as AWSPENDING for rotation of secret id.")
  }

  "handler createSecret" should "return an error if the current secret doesn't exist" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val config = Config("http://localhost")
    val userClient = mock[UserClient[IO]]
    val secretsManagerClient = mockSecretsManagerClient()

    when(secretsManagerClient.getSecretValue[String](any[Stage])(using any[Decoder[String]]))
      .thenReturn(IO.raiseError(new Exception("Cannot find secret id")))
    val dependencies = Dependencies(secretId => IO.pure(userClient), secretId => secretsManagerClient)

    val ex = new Lambda().handler(rotationEvent, config, dependencies).attempt.unsafeRunSync().left.value

    ex.getMessage should equal("Cannot find secret id")
  }
  
  "handler createSecret" should "not call putSecretValue if a Pending value already exists" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val config = Config("http://localhost")
    val userClient = mock[UserClient[IO]]
    val secretsManagerClient = mockSecretsManagerClient()

    when(secretsManagerClient.getSecretValue[Map[String, String]](any[Stage])(using any[Decoder[Map[String, String]]]))
      .thenReturn(IO(Map("user" -> "secret")))
    when(secretsManagerClient.getSecretValue[Map[String, String]](any[String], any[Stage])(using any[Decoder[Map[String, String]]]))
      .thenReturn(IO(Map("user" -> "pendingSecret")))
    val dependencies = Dependencies(secretId => IO.pure(userClient), secretId => secretsManagerClient)

    new Lambda().handler(rotationEvent, config, dependencies).unsafeRunSync()

    verify(secretsManagerClient, times(0)).putSecretValue(any[String], any[Stage], any[Option[String]])(using any[Encoder[String]])
  }

  "handler createSecret" should "putSecretValue if a Pending value does not already exist" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val config = Config("http://localhost")
    val userClient = mock[UserClient[IO]]
    val secretsManagerClient = mockSecretsManagerClient()

    when(secretsManagerClient.getSecretValue[Map[String, String]](any[Stage])(using any[Decoder[Map[String, String]]]))
      .thenReturn(IO(Map("user" -> "secret")))
    when(secretsManagerClient.getSecretValue[Map[String, String]](any[String], any[Stage])(using any[Decoder[Map[String, String]]]))
      .thenReturn(IO.raiseError(new Exception("Pending secret does not exist")))
    when(secretsManagerClient.generateRandomPassword(any[Int], any[String])).thenReturn(IO("newPassword"))
    when(secretsManagerClient.putSecretValue(any[String], any[Stage], any[Option[String]])(using any[Encoder[String]]))
      .thenReturn(IO(PutSecretValueResponse.builder.build))
    val dependencies = Dependencies(secretId => IO.pure(userClient), secretId => secretsManagerClient)

    new Lambda().handler(rotationEvent, config, dependencies).unsafeRunSync()

    verify(secretsManagerClient, times(1)).putSecretValue[Map[String, String]](
      ArgumentMatchers.eq(Map("user" -> "newPassword")),
      argThat[Stage] {
        case Pending => true
        case _       => true
      },
      ArgumentMatchers.eq(Option("token"))
    )(using any[Encoder[Map[String, String]]])
  }

  "handler setSecret" should "call reset password with the correct credentials" in {
    val rotationEvent = RotationEvent(SetSecret, "id", "token")
    val config = Config("http://localhost")
    val userClient = mock[UserClient[IO]]
    val secretsManagerClient = mockSecretsManagerClient()

    val resetPasswordCaptor: ArgumentCaptor[ResetPasswordRequest] = ArgumentCaptor.forClass(classOf[ResetPasswordRequest])
    when(secretsManagerClient.getSecretValue[Map[String, String]](argThat[Stage] {
      case Pending => false
      case _       => true
    })(using any[Decoder[Map[String, String]]]))
      .thenReturn(IO(Map("apiUser" -> "secret")))
    when(secretsManagerClient.getSecretValue[Map[String, String]](argThat[Stage] {
      case Pending => true
      case _       => false
    })(using any[Decoder[Map[String, String]]]))
      .thenReturn(IO(Map("apiUser" -> "newSecret")))

    when(userClient.resetPassword(resetPasswordCaptor.capture())).thenReturn(IO.unit)
    val dependencies = Dependencies(secretId => IO.pure(userClient), secretId => secretsManagerClient)

    new Lambda().handler(rotationEvent, config, dependencies).unsafeRunSync()

    resetPasswordCaptor.getValue.password should equal("secret")
    resetPasswordCaptor.getValue.newPassword should equal("newSecret")
  }

  "handler testSecret" should "call the testSecret method in UserClient" in {
    val rotationEvent = RotationEvent(TestSecret, "id", "token")
    val config = Config("http://localhost")
    val userClient = mock[UserClient[IO]]
    val secretsManagerClient = mockSecretsManagerClient()

    when(userClient.testNewPassword()).thenReturn(IO.unit)
    val dependencies = Dependencies(secretId => IO.pure(userClient), secretId => secretsManagerClient)

    new Lambda().handler(rotationEvent, config, dependencies).unsafeRunSync()

    verify(userClient, times(1)).testNewPassword()
  }

  "handler finishSecret" should "return an error if the pending secret has no version" in {
    val rotationEvent = RotationEvent(FinishSecret, "id", "newVersion")
    val config = Config("http://localhost")
    val userClient = mock[UserClient[IO]]

    val moveToCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val moveFromCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    val secretsManagerClient = mockSecretsManagerClient(Map("currentVersion" -> List("AWSCURRENT"), "newVersion" -> List("AWSPENDING")))

    when(secretsManagerClient.updateSecretVersionStage(moveToCaptor.capture, moveFromCaptor.capture, any[Stage])).thenReturn(IO(UpdateSecretVersionStageResponse.builder.build))

    val dependencies = Dependencies(secretId => IO.pure(userClient), secretId => secretsManagerClient)

    new Lambda().handler(rotationEvent, config, dependencies).unsafeRunSync()

    moveToCaptor.getValue should equal("newVersion")
    moveFromCaptor.getValue should equal("currentVersion")
  }
}
