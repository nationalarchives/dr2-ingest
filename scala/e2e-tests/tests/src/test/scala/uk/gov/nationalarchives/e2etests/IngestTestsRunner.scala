package uk.gov.nationalarchives.e2etests

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import org.scalatest.ParallelTestExecution
import org.scalatest.funspec.AnyFunSpec

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class IngestTestsRunner extends AnyFunSpec with ParallelTestExecution {

  def loadAllScenarios(): List[String] = Files
    .list(Path.of(getClass.getResource("/features").getPath))
    .toList
    .asScala
    .map(Files.readString)
    .toList
    .flatMap { feature =>
      val scenarios = feature.split("\n\n")
      info(scenarios.head)
      scenarios.tail.toList
    }

  def processEachStep(steps: List[String], stepDefs: StepDefs[IO])(using ref: Ref[IO, List[UUID]]): IO[Unit] =
    steps.traverse { line =>
      line.trim().split(" ").toList match
        case keyword :: expressionParts =>
          val expression = expressionParts.mkString(" ")
          keyword match
            case "Given" => testGiven(expression, stepDefs)
            case "When"  => testWhen(expression, stepDefs)
            case "Then"  => testThen(expression, stepDefs)
            case invalid => IO.raiseError(new Exception(s"Invalid step $invalid"))
        case Nil => IO.raiseError(new Exception("Invalid step definition"))
    }.void

  def runIngestTests(stepDefs: StepDefs[IO]): Unit =
    loadAllScenarios().map { test =>
      val steps = test.split("\n").toList
      runTestFunction(steps.head) {
        processEachStep(steps.tail, stepDefs)
      }
    }.head

  def runTestFunction(specText: String)(testFun: Ref[IO, List[UUID]] ?=> IO[Unit]): Unit =
    it(specText) {
      Ref.of[IO, List[UUID]](Nil).flatMap(ref => testFun(using ref)).unsafeRunSync()
    }

  def testGiven(s: String, stepDefs: StepDefs[IO])(using ref: Ref[IO, List[UUID]]): IO[Unit] = {
    info(s"Given $s")
    s match
      case s"An ingest with $count file with invalid metadata"      => stepDefs.createFiles(count.toInt, invalidMetadata = true)
      case s"An ingest with $count file$_ with an empty checksum"   => stepDefs.createFiles(count.toInt, true)
      case s"An ingest with $count file$_ with an invalid checksum" => stepDefs.createFiles(count.toInt, invalidChecksum = true)
      case s"An ingest with $count file$_"                          => stepDefs.createFiles(count.toInt)
  }

  def testWhen(s: String, stepDefs: StepDefs[IO])(using ref: Ref[IO, List[UUID]]): IO[Unit] =
    info(s"When $s")
    s match
      case "I send messages to the input queue" => stepDefs.sendMessages(ref)
      case "I create a batch with this file"    => stepDefs.createBatch(ref)

  def testThen(s: String, stepDefs: StepDefs[IO])(using ref: Ref[IO, List[UUID]]): IO[Unit] =
    info(s"Then $s")
    s match
      case "I receive the ingest complete messages"     => stepDefs.waitForIngestCompleteMessages(ref)
      case s"I wait for $count $unit"                   => IO.sleep(Duration.create(count.toInt, unit))
      case "I receive an ingest error message"          => stepDefs.waitForIngestErrorMessages(ref)
      case "I receive an error in the validation queue" => stepDefs.pollForTDRValidationMessages(ref)
}
