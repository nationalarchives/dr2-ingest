import sbt.*

object Dependencies {
  private val mockitoScalaVersion = "1.17.22"
  private val awsLibraryVersion = "1.12.555"
  lazy val log4jVersion = "2.20.0"

  lazy val authUtils = "uk.gov.nationalarchives" %% "tdr-auth-utils" % "0.0.141"
  lazy val awsDynamoDbClient = "uk.gov.nationalarchives" %% "da-dynamodb-client" % "0.1.22"
  lazy val awsJavaEvents = "com.amazonaws" % "aws-lambda-java-events" % "3.11.3"
  lazy val awsLambda = "com.amazonaws" % "aws-java-sdk-lambda" % awsLibraryVersion
  lazy val awsLambdaCore = "com.amazonaws" % "aws-lambda-java-core" % "1.2.3"
  lazy val awsSnsClient = "uk.gov.nationalarchives" %% "da-sns-client" % "0.1.22"
  lazy val awsSecretsManager = "com.amazonaws" % "aws-java-sdk-secretsmanager" % awsLibraryVersion
  lazy val catsEffect = "org.typelevel" %% "cats-effect" % "3.5.1"
  lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % log4jVersion
  lazy val log4jTemplateJson = "org.apache.logging.log4j" % "log4j-layout-template-json" % log4jVersion
  lazy val mockitoScala = "org.mockito" %% "mockito-scala" % mockitoScalaVersion
  lazy val mockitoScalaTest = "org.mockito" %% "mockito-scala-scalatest" % mockitoScalaVersion
  lazy val preservicaClient = "uk.gov.nationalarchives" %% "preservica-client-fs2" % "0.0.18"
  lazy val pureConfigCats = "com.github.pureconfig" %% "pureconfig-cats-effect" % "0.17.4"
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig" % "0.17.4"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.17"
  lazy val sttpClient = "com.softwaremill.sttp.client3" %% "core" % "3.9.0"
  lazy val typeSafeConfig = "com.typesafe" % "config" % "1.4.2"
  lazy val wiremock = "com.github.tomakehurst" % "wiremock-jre8" % "3.0.1"
}
