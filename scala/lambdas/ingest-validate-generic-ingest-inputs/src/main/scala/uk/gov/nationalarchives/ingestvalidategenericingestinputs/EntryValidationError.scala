package uk.gov.nationalarchives.ingestvalidategenericingestinputs

object EntryValidationError {
  trait ValidationError:
    val errorMessage: String

  trait ValueError extends ValidationError:
    val valueThatCausedError: String

  sealed trait SchemaValidationError extends ValidationError

  case class AtLeastOneEntryWithSeriesAndNullParentError(errorMessage: String) extends SchemaValidationError

  case class MinimumAssetsAndFilesError(errorMessage: String) extends SchemaValidationError

  case class MissingPropertyError(propertyWithError: String, errorMessage: String) extends SchemaValidationError

  case class SchemaValueError(valueThatCausedError: String, errorMessage: String) extends SchemaValidationError with ValueError
}
