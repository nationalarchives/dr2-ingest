package uk.gov.nationalarchives.preingesttdraggregator

import io.circe.Json

object Duration:
  opaque type Seconds = Int

  opaque type MilliSeconds = Long

  object Seconds:
    def apply(length: Int): Seconds = length

  object MilliSeconds:
    def apply(length: Long): MilliSeconds = length

  extension (s: Seconds)
    def *(other: Int): Int = s * other
    def length: Int = s
    def toJson: Json = Json.fromInt(s)

  extension (m: MilliSeconds)
    def length: Long = m
    def +(other: Long): Long = m + other
end Duration
