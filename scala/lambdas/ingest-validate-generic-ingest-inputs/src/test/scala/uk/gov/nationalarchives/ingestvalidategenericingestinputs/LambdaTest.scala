package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.testUtils.LambdaTestUtils.*

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.*

class LambdaTest extends AnyFunSpec with TableDrivenPropertyChecks:

  val inputFiles: TableFor1[String] = Table(
    "inputFileName",
    Files.list(Paths.get(getClass.getResource("/inputjson").toURI)).toList.asScala.map(_.toFile.getName).toList*
  )

  describe("handler") {
    forAll(inputFiles) { inputFileName =>
      it(s"should validate the input file $inputFileName") {
        val (expectedErrors, errors) = runLambda(inputFileName)
        expectedErrors should equal(errors)
      }
    }
  }
end LambdaTest
