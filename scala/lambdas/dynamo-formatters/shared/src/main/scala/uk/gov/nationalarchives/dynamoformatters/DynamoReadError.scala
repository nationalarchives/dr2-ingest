package uk.gov.nationalarchives.dynamoformatters

import cats.Show
import cats.data.NonEmptyList
import dynosaur.DynamoValue
import uk.gov.nationalarchives.dynamoformatters.DynamoReadError.*

sealed trait DynamoReadError:
  def describe(d: DynamoReadError): String = {
    d match
      case InvalidPropertiesError(problems) =>
        problems.toList.map(p => s"'${p._1}': ${describe(p._2)}").mkString(", ")
      case NoPropertyOfType(propertyType, actual) => s"not of type: '$propertyType' was '$actual'"
      case TypeCoercionError(e)                   => s"could not be converted to desired type: $e"
      case MissingProperty                        => "missing"
  }

object DynamoReadError:
  given Show[DynamoReadError] = Show.show[DynamoReadError](err => err.describe(err))

  case object MissingProperty extends DynamoReadError

  case class TypeCoercionError(err: Throwable) extends DynamoReadError

  case object InvalidProperty extends DynamoReadError

  case class NoPropertyOfType(s: String, dynamoValue: DynamoValue) extends DynamoReadError

  final case class InvalidPropertiesError(errors: NonEmptyList[(String, DynamoReadError)]) extends DynamoReadError
