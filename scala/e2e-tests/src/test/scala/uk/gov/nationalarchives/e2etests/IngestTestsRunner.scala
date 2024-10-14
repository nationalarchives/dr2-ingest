package uk.gov.nationalarchives.e2etests

import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import org.scalatest.ParallelTestExecution
import org.scalatest.funspec.AnyFunSpec
import uk.gov.nationalarchives.e2etests.IngestTestsRunner.*
import uk.gov.nationalarchives.e2etests.StepDefs.SqsMessage

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

trait IngestTestsRunner extends AnyFunSpec with ParallelTestExecution {

  def runIngestTests(): Unit =
    Files.list(Path.of(getClass.getResource("/features").getPath))
      .toList.asScala
      .map(Files.readString)
      .toList
      .flatMap { eachString =>
        val eachTest = eachString.split("\n\n")
        eachTest.toList.map { test =>
          val eachTestLine = test.split("\n").toList
          ingest(eachTestLine.head) {
            eachTestLine.tail.traverse { line =>
              line.split(" ").toList match
                case head :: next =>
                  val testString = next.mkString(" ")
                  head match
                    case "Given" => testGiven(testString)
                    case "When" => testWhen(testString)
                    case "Then" => testThen(testString)
                    case invalid => IO.raiseError(new Exception(s"Invalid step $invalid"))
                case Nil => IO.unit
            }.void
          }
        }
      }.head

  def ingest(specText: String)(testFun: Ref[IO, List[UUID]] ?=> IO[Unit]): Unit =
    it(specText) {
      Ref.of[IO, List[UUID]](Nil).flatMap(ref => testFun(using ref)).unsafeRunSync()
      }

  def testGiven(s: String)(using ref: Ref[IO, List[UUID]]): IO[Unit] = {
    info(s"Given $s")
    s match
      case s"An ingest with $count file with invalid metadata" => stepDefs.createFiles(count.toInt, invalidMetadata = true)
      case s"An ingest with $count file$_ with an empty checksum" => stepDefs.createFiles(count.toInt, true)
      case s"An ingest with $count file$_ with an invalid checksum" => stepDefs.createFiles(count.toInt, invalidChecksum = true)
      case s"An ingest with $count file$_" => stepDefs.createFiles(count.toInt)
  }

  def testWhen(s: String)(using ref: Ref[IO, List[UUID]]): IO[Unit] =
    info(s"When $s")
    s match
      case s"I send messages to the input queue" => stepDefs.sendMessages(ref)
      case "I create a batch with this file" => stepDefs.createBatch(ref)


  def testThen(s: String)(using ref: Ref[IO, List[UUID]]): IO[Unit] =
    info(s"Then $s")
    s match
      case "I receive the ingest complete messages" => stepDefs.waitForIngestCompleteMessages(ref)
      case s"I wait for $count minutes" => IO.sleep(Duration.create(count.toInt, "minutes"))
      case "I receive an ingest error message" => stepDefs.waitForIngestErrorMessages(ref)
      case "I receive an error in the validation queue" => stepDefs.pollForTDRValidationMessages(ref)
}

object IngestTestsRunner:
  val stepDefs: StepDefs[IO] = (for {
    messageUuidCell <- AtomicCell[IO].empty[List[SqsMessage]]
  } yield StepDefs[IO](messageUuidCell)).unsafeRunSync()
