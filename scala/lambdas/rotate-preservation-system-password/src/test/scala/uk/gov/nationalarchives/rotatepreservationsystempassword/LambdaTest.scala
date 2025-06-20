package uk.gov.nationalarchives.rotatepreservationsystempassword

import cats.syntax.all.*
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.rotatepreservationsystempassword.Lambda.*
import uk.gov.nationalarchives.rotatepreservationsystempassword.Lambda.RotationStep.*
import uk.gov.nationalarchives.rotatepreservationsystempassword.TestUtils.*
import uk.gov.nationalarchives.DASecretsManagerClient.Stage.*

class LambdaTest extends AnyFlatSpec with EitherValues {

  "handler" should "fail if rotation is not enabled" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val (_, _, res) = runLambda(rotationEvent, Secret(Map.empty, false), Credentials("", ""))

    res.left.value.getMessage should equal("Secret id is not enabled for rotation.")
  }

  "handler" should "fail if there are no stages for the secret version" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val secret = Secret(Map("anotherToken" -> List(SecretStage(None, Current))))

    val (_, _, res) = runLambda(rotationEvent, secret, Credentials("", ""))

    res.left.value.getMessage should equal("Secret version token has no stage set for rotation of secret id.")
  }

  "handler" should "fail if the version is already current" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val secret = Secret(Map("token" -> List(SecretStage(None, Current))))

    val (_, _, res) = runLambda(rotationEvent, secret, Credentials("", ""))

    res.left.value.getMessage should equal("Secret id is already at AWSCURRENT.")
  }

  "handler" should "fail if the version does not have a pending stage" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val secret = Secret(Map("token" -> Nil))

    val (_, _, res) = runLambda(rotationEvent, secret, Credentials("", ""))

    res.left.value.getMessage should equal("Secret version token not set as AWSPENDING for rotation of secret id.")
  }

  "handler createSecret" should "return an error if there is an error getting the secret" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val secret = Secret(Map("token" -> List(SecretStage(None, Pending))))

    val (_, _, res) = runLambda(rotationEvent, secret, Credentials("", ""), Errors(getSecret = true).some)

    res.left.value.getMessage should equal("Error getting secret")
  }

  "handler createSecret" should "not call putSecretValue if a Pending value already exists" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val secret = Secret(Map("token" -> List(SecretStage(Option(AuthDetails("username", "password", "")), Pending))))

    val (secretResult, _, _) = runLambda(rotationEvent, secret, Credentials("", ""))

    secretResult.versionToStage.size should equal(1)
    secretResult.versionToStage("token").head.value should equal(Option(AuthDetails("username", "password", "")))

  }

  "handler createSecret" should "create the secret value if a Pending value does not already exist" in {
    val rotationEvent = RotationEvent(CreateSecret, "id", "token")
    val versionToStage = Map(
      "token" -> List(SecretStage(None, Pending)),
      "anotherToken" -> List(SecretStage(Option(AuthDetails("user", "currentSecret", "")), Current))
    )
    val secret = Secret(versionToStage)

    val (secretResult, _, _) = runLambda(rotationEvent, secret, Credentials("", ""))

    val newPassword = secretResult.versionToStage("token").head.value.get.password
    newPassword.length should equal(15)
  }

  "handler setSecret" should "call reset password with the correct credentials" in {
    val rotationEvent = RotationEvent(SetSecret, "id", "token")
    val versionToStage = Map(
      "token" -> List(SecretStage(Option(AuthDetails("user", "pendingSecret", "")), Pending)),
      "anotherToken" -> List(SecretStage(Option(AuthDetails("user", "currentSecret", "")), Current))
    )
    val secret = Secret(versionToStage)

    val (_, credentials, res) = runLambda(rotationEvent, secret, Credentials("", ""))

    credentials.oldPassword should equal("currentSecret")
    credentials.newPassword should equal("pendingSecret")
  }

  "handler setSecret" should "remove the previous version if there is an error resetting the password" in {
    val rotationEvent = RotationEvent(SetSecret, "id", "token")
    val versionToStage = Map(
      "token" -> List(SecretStage(Option(AuthDetails("user", "pendingSecret", "")), Pending)),
      "anotherToken" -> List(SecretStage(Option(AuthDetails("user", "currentSecret", "")), Current))
    )
    val secret = Secret(versionToStage)

    val (secretResponse, _, _) = runLambda(rotationEvent, secret, Credentials("", "", resetSuccess = false))
    secretResponse.versionToStage("token").size should equal(0)
  }

  "handler testSecret" should "return an error if test secret fails" in {
    val rotationEvent = RotationEvent(TestSecret, "id", "token")

    val secret = Secret(Map("token" -> List(SecretStage(None, Pending))))
    val (_, _, res) = runLambda(rotationEvent, secret, Credentials("", "", testSuccess = false))

    res.left.value.getMessage should equal("Password test failed")
  }

  "handler finishSecret" should "update the secret version" in {
    val rotationEvent = RotationEvent(FinishSecret, "id", "token")
    val pendingStages = List(SecretStage(Option(AuthDetails("user", "pendingSecret", "")), Pending))
    val currentStages = List(SecretStage(Option(AuthDetails("user", "currentSecret", "")), Current))
    val versionToStage = Map("token" -> pendingStages, "anotherToken" -> currentStages)
    val secret = Secret(versionToStage)

    val (secretResult, _, res) = runLambda(rotationEvent, secret, Credentials("", ""))
    val expectedResult = Secret(Map("token" -> (currentStages ++ pendingStages), "anotherToken" -> Nil))

    secretResult should equal(expectedResult)
  }
}
