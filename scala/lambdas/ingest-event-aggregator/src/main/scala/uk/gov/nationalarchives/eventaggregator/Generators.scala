package uk.gov.nationalarchives.eventaggregator

import cats.effect.kernel.Async

import java.time.Instant
import java.util.UUID

trait Generators[F[_]: Async]:
  def generateUuid: UUID
  def generateInstant: Instant

object Generators:
  def apply[F[_]: Async](using ev: Generators[F]): Generators[F] = ev

  given uuidGenerator[F[_]: Async]: Generators[F] = new Generators[F]:
    override def generateUuid: UUID = UUID.randomUUID

    override def generateInstant: Instant = Instant.now
