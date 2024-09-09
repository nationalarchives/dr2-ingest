package uk.gov.nationalarchives.preingesttdraggregator

import io.circe.Json

object Duration:
  opaque type Seconds = Int

  opaque type Milliseconds = Long

  object Seconds:
    def apply(length: Int): Seconds = length

  object MilliSeconds:
    def apply(length: Long): Milliseconds = length

  extension (s: Seconds)
    def *(other: Int): Int = s * other
    def length: Int = s
    def toJson: Json = Json.fromInt(s)
    def toMilliSeconds: Milliseconds = s * 1000

  extension (m: Milliseconds)
    def length: Long = m
    def +(other: Milliseconds): Milliseconds = m + other
    def -(other: Milliseconds): Long = m - other

  extension (i: Int) def seconds: Seconds = Seconds(i)

  extension (l: Long) def milliSeconds: Milliseconds = MilliSeconds(l)

end Duration
