import sbt._
object Dependencies {
  lazy val logbackVersion = "2.23.1"
  lazy val pureConfigVersion = "0.17.6"
  lazy val daAwsClientsVersion = "0.1.47"
  private val fs2Version = "3.10.1"
  private val sttpVersion = "3.9.5"
  private val circeVersion = "0.14.6"
  private val log4CatsVersion = "2.6.0"
  private val awsLibraryVersion = "1.12.686"
  private lazy val scalaTestVersion = "3.2.18"

  lazy val awsCrt = "software.amazon.awssdk.crt" % "aws-crt" % "0.29.11"
  lazy val awsLambda = "com.amazonaws" % "aws-java-sdk-lambda" % awsLibraryVersion
  lazy val awsSecretsManager = "com.amazonaws" % "aws-java-sdk-secretsmanager" % awsLibraryVersion
  lazy val catsEffect = "org.typelevel" %% "cats-effect" % "3.5.4"
  lazy val circeCore = "io.circe" %% "circe-core" % circeVersion
  lazy val circeGeneric = "io.circe" %% "circe-generic" % circeVersion
  lazy val circeParser = "io.circe" %% "circe-parser" % circeVersion
  lazy val commonsCompress = "org.apache.commons" % "commons-compress" % "1.26.1"
  lazy val dynamoClient = "uk.gov.nationalarchives" %% "da-dynamodb-client" % daAwsClientsVersion
  lazy val dynamoFormatters = "uk.gov.nationalarchives" %% "dynamo-formatters" % "0.0.12"
  lazy val eventBridgeClient = "uk.gov.nationalarchives" %% "da-eventbridge-client" % daAwsClientsVersion
  lazy val fs2Core = "co.fs2" %% "fs2-core" % fs2Version
  lazy val fs2IO = "co.fs2" %% "fs2-io" % fs2Version
  lazy val fs2Reactive = "co.fs2" %% "fs2-reactive-streams" % fs2Version
  lazy val jaxb = "javax.xml.bind" % "jaxb-api" % "2.3.1"
  lazy val lambdaCore = "com.amazonaws" % "aws-lambda-java-core" % "1.2.3"
  lazy val lambdaJavaEvents = "com.amazonaws" % "aws-lambda-java-events" % "3.11.4"
  lazy val log4CatsCore = "org.typelevel" %% "log4cats-core" % log4CatsVersion;
  lazy val log4CatsSlf4j = "org.typelevel" %% "log4cats-slf4j" % log4CatsVersion
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % logbackVersion
  lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j-impl" % logbackVersion
  lazy val log4jTemplateJson = "org.apache.logging.log4j" % "log4j-layout-template-json" % logbackVersion
  lazy val mockito = "org.scalatestplus" %% "mockito-5-10" % s"$scalaTestVersion.0"
  lazy val preservicaClient = "uk.gov.nationalarchives" %% "preservica-client-fs2" % "0.0.63"
  lazy val pureConfigCats = "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig-core" % pureConfigVersion
  lazy val reactorTest = "io.projectreactor" % "reactor-test" % "3.6.4"
  lazy val s3Client = "uk.gov.nationalarchives" %% "da-s3-client" % daAwsClientsVersion
  lazy val scalaParserCombinators = "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion
  lazy val scalaXml = "org.scala-lang.modules" %% "scala-xml" % "2.2.0"
  lazy val sfnClient = "uk.gov.nationalarchives" %% "da-sfn-client" % daAwsClientsVersion
  lazy val snsClient = "uk.gov.nationalarchives" %% "da-sns-client" % daAwsClientsVersion
  lazy val sttpClient = "com.softwaremill.sttp.client3" %% "core" % sttpVersion
  lazy val sttpClientFs2 = "com.softwaremill.sttp.client3" %% "fs2" % sttpVersion
  lazy val sttpCirce = "com.softwaremill.sttp.client3" %% "circe" % sttpVersion
  lazy val typeSafeConfig = "com.typesafe" % "config" % "1.4.3"
  lazy val upickle = "com.lihaoyi" %% "upickle" % "3.2.0"
  lazy val wiremock = "com.github.tomakehurst" % "wiremock" % "3.0.1"

}
