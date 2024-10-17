package uk.gov.nationalarchives.e2etests

import cats.effect.IO
import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import uk.gov.nationalarchives.e2etests.StepDefs.SqsMessage

class IngestTests extends IngestTestsRunner:
  val stepDefs: StepDefs[IO] = (for {
    messageUuidCell <- AtomicCell[IO].empty[List[SqsMessage]]
  } yield StepDefs[IO](messageUuidCell)).unsafeRunSync()

  runIngestTests(stepDefs)
