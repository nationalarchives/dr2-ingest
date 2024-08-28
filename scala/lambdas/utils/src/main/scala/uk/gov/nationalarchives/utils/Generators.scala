package uk.gov.nationalarchives.utils

import cats.effect.kernel.Async

import java.time.Instant
import java.util.UUID

trait Generators[F[_]: Async]:
  def generateRandomUuid: UUID
  def generateInstant: Instant

object Generators:
  def apply[F[_]: Async](using ev: Generators[F]): Generators[F] = ev

  given uuidGenerator[F[_]: Async]: Generators[F] = new Generators[F]:
    override def generateRandomUuid: UUID = UUID.randomUUID

    override def generateInstant: Instant = Instant.now
