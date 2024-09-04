package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.data.ValidatedNel
import cats.kernel.Semigroup
import ujson.Value
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.EntryValidationError.ValidationError

object ValidatedUtils {
  // Not expecting this 'given' to be used since we are only combining errors, but Validate's 'combine' method needs it
  given combineValues: Semigroup[Value] =
    new Semigroup[Value] {
      override def combine(val1: Value, val2: Value): Value = val1
    }

  type Entry = Map[String, ValidatedNel[ValidationError, Value]]
}
