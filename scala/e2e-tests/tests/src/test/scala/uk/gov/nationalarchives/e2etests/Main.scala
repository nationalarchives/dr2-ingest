package uk.gov.nationalarchives.e2etests

import cats.syntax.all.*
import cats.effect.std.AtomicCell
import cats.effect.{ExitCode, IO, IOApp, Ref}
import uk.gov.nationalarchives.e2etests.StepDefs.SqsMessage

import java.util.UUID

object Main extends IOApp {

  val total = 5

  override def run(args: List[String]): IO[ExitCode] =
    for {
      cell <- AtomicCell[IO].of[List[SqsMessage]](Nil)
      stepDefs = StepDefs[IO](cell)
      _ <- (1 to total).toList.traverse(num => {
        val prefix = if num <= total / 3 then "TDR" else if num <= total / 3 * 2 then "DEFAULT" else "COURTDOC"
        Ref.of[IO, List[UUID]](Nil).flatMap { ref =>
          stepDefs.createFiles(1)(using ref) >> create(stepDefs, prefix)(using ref)
        }
      })
    } yield ExitCode.Success

  def create(stepDefs: StepDefs[IO], prefix: String)(using ref: Ref[IO, List[UUID]]): IO[Unit] = {
    for {
      ids <- ref.get
      _ <- ids.traverse(id => stepDefs.createBatch(ref, prefix))
    } yield ()
  }
}
