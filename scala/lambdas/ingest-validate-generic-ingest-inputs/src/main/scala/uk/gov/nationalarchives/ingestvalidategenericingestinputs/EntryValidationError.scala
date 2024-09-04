package uk.gov.nationalarchives.ingestvalidategenericingestinputs

object EntryValidationError {
  trait ValidationError:
    val errorMessage: String

  sealed trait SchemaValidationError extends ValidationError

  sealed trait SchemaValidationEntryError extends ValidationError:
    val propertyWithError: String

  case class AtLeastOneEntryWithSeriesAndNullParentError(errorMessage: String) extends SchemaValidationError

  case class MinimumAssetsAndFilesError(errorMessage: String) extends SchemaValidationError

  case class MissingPropertyError(propertyWithError: String, errorMessage: String) extends SchemaValidationEntryError

  case class ValueError(propertyWithError: String, valueThatCausedError: String, errorMessage: String) extends SchemaValidationEntryError
}
