package uk.gov.nationalarchives.utils
import cats.effect.IO
import io.circe.Printer
import io.circe.parser.decode
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import org.scalatest.EitherValues
import pureconfig.generic.derivation.default.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import pureconfig.ConfigReader

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class LambdaRunnerTest extends AnyFlatSpec with EitherValues {
  def inputStream = new ByteArrayInputStream(Input("input").asJson.printWith(Printer.noSpaces).getBytes)
  case class TestDependency(returnValue: String) {
    def callMethod: String = returnValue
  }

  case class Input(inputValue: String)
  case class Output(inputCopy: String, configCopy: String, dependencyCopy: String)
  case class Config(configValue: String, dependencyReturnValue: String) derives ConfigReader
  case class Dependencies(testDependency: TestDependency)
  case class EmptyDependencies()

  "handleRequest" should "write the correct output to the output stream" in {
    val lambdaRunner = new LambdaRunner[Input, Output, Config, Dependencies]() {
      override def handler: (Input, Config, Dependencies) => IO[Output] = (input, config, dependencies) => {
        IO.pure(Output(input.inputValue, config.configValue, dependencies.testDependency.callMethod))
      }

      override def dependencies(config: Config): IO[Dependencies] = IO.pure(Dependencies(TestDependency(config.dependencyReturnValue)))
    }
    val os = new ByteArrayOutputStream()
    lambdaRunner.handleRequest(inputStream, os, null)

    val output = decode[Output](os.toByteArray.map(_.toChar).mkString).value
    output.inputCopy should equal("input")
    output.dependencyCopy should equal("dependencyReturnValue")
    output.configCopy should equal("configValue")
  }

  "handleRequest" should "write nothing to the output stream if Unit is provided as the output type" in {
    val lambdaRunner = new LambdaRunner[Input, Unit, Config, EmptyDependencies]() {
      override def handler: (Input, Config, EmptyDependencies) => IO[Unit] = (_, _, _) => IO.unit
      override def dependencies(config: Config): IO[EmptyDependencies] = IO.pure(EmptyDependencies())
    }

    val os = new ByteArrayOutputStream()
    lambdaRunner.handleRequest(inputStream, os, null)

    os.toByteArray.length should equal(0)
  }

  "handleRequest" should "return an error if the config object is invalid" in {
    case class InvalidConfig(invalidParameter: Int) derives ConfigReader
    val lambdaRunner = new LambdaRunner[Input, Unit, InvalidConfig, EmptyDependencies]() {
      override def handler: (Input, InvalidConfig, EmptyDependencies) => IO[Unit] = (_, _, _) => IO.unit
      override def dependencies(config: InvalidConfig): IO[EmptyDependencies] = IO.pure(EmptyDependencies())
    }

    val os = new ByteArrayOutputStream()
    val ex = intercept[Exception] {
      lambdaRunner.handleRequest(inputStream, os, null)
    }
    ex.getMessage.contains("Key not found: 'invalid-parameter'") should equal(true)
  }

  "handleRequest" should "return an error if the dependencies method returns an error" in {
    val lambdaRunner = new LambdaRunner[Input, Unit, Config, EmptyDependencies]() {
      override def handler: (Input, Config, EmptyDependencies) => IO[Unit] = (_, _, _) => IO.unit
      override def dependencies(config: Config): IO[EmptyDependencies] = IO.raiseError(new Exception("Error creating dependencies"))
    }

    val os = new ByteArrayOutputStream()

    val ex = intercept[Exception] {
      lambdaRunner.handleRequest(inputStream, os, null)
    }
    ex.getMessage should equal("Error creating dependencies")
  }

  "handleRequest" should "return an error if the handler method returns an error" in {
    val lambdaRunner = new LambdaRunner[Input, Unit, Config, EmptyDependencies]() {
      override def handler: (Input, Config, EmptyDependencies) => IO[Unit] = (_, _, _) => IO.raiseError(new Exception("Error running handler"))
      override def dependencies(config: Config): IO[EmptyDependencies] = IO.pure(EmptyDependencies())
    }

    val os = new ByteArrayOutputStream()

    val ex = intercept[Exception] {
      lambdaRunner.handleRequest(inputStream, os, null)
    }
    ex.getMessage should equal("Error running handler")
  }
}
