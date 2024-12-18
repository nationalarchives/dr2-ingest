package uk.gov.nationalarchives.utils

import java.time.Instant
import java.util.UUID
import scala.util.Random

trait Generators:
  def generateRandomUuid: UUID
  def generateInstant: Instant
  def generateRandomInt(startRange: Int, endRange: Int): Int

object Generators:
  def apply()(using ev: Generators): Generators = ev

  given uuidGenerator[F[_]]: Generators = new Generators:
    override def generateRandomUuid: UUID = UUID.randomUUID

    override def generateInstant: Instant = Instant.now

    override def generateRandomInt(minInclusive: Int = 1, maxExclusive: Int = 101): Int = new Random().between(minInclusive, maxExclusive)
