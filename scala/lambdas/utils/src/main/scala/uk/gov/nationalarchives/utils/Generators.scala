package uk.gov.nationalarchives.utils

import java.time.Instant
import java.util.UUID

trait Generators:
  def generateRandomUuid: UUID
  def generateInstant: Instant

object Generators:
  def apply()(using ev: Generators): Generators = ev

  given uuidGenerator[F[_]]: Generators = new Generators:
    override def generateRandomUuid: UUID = UUID.randomUUID

    override def generateInstant: Instant = Instant.now
