package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.*
import cats.effect.IO
import cats.syntax.all.*
import fs2.interop.reactivestreams.*
import fs2.{Chunk, Stream}
import io.circe.*
import io.circe.derivation.Configuration
import io.circe.generic.semiauto.*
import org.reactivestreams.Publisher
import pureconfig.ConfigReader
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.utils.ExternalUtils.*

import java.net.URI
import java.nio.ByteBuffer
import java.util.UUID
import scala.jdk.CollectionConverters.*

object Utils:

  extension (pub: Publisher[ByteBuffer])
    def toByteStream: Stream[IO, Byte] = pub
      .toStreamBuffered[IO](1024)
      .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))

  trait ErrorMessage

  object ErrorMessage:
    given Show[ErrorMessage] = Show[ErrorMessage] {
      case ParentTypeInvalid(parentTypeOpt, fileTypeOpt, id) => s"Parent type ${parentTypeOpt.orNull} is not valid for file type ${fileTypeOpt.orNull} for id $id"
      case CircularDependency(id, existingParents)           => s"Circular dependency with id $id -> ${existingParents.filter(_ != id).mkString(" -> ")} -> $id"
      case NoAssetChildren(id)                               => s"Asset $id has no children"
      case NoTopLevelFolder                                  => "There is no top level folder in this metadata file"
      case NoAsset                                           => "There is not at least one asset in this metadata file"
      case NoFile                                            => "There is not at least one file in this metadata file"
      case MissingParent(missingParentId)                    => s"Parent id $missingParentId does not exist in the json file"

    }

    case class ParentTypeInvalid(parentTypeOpt: Option[Type], fileTypeOpt: Option[Type], id: UUID) extends ErrorMessage

    case class CircularDependency(id: UUID, existingParents: List[UUID]) extends ErrorMessage

    case class NoAssetChildren(id: UUID) extends ErrorMessage

    case object NoTopLevelFolder extends ErrorMessage

    case object NoAsset extends ErrorMessage

    case object NoFile extends ErrorMessage

    case class MissingParent(missingParentId: UUID) extends ErrorMessage

    given Encoder[ErrorMessage] = Encoder.encodeString.contramap[ErrorMessage](_.show)
    given Encoder[ObjectValidationResult] = deriveEncoder[ObjectValidationResult]
    given Encoder[WholeFileValidationResult] = deriveEncoder[WholeFileValidationResult]

    case class ObjectValidationError(results: List[ObjectValidationResult]) extends Throwable
    case class ObjectValidationResult(json: Json, errors: List[String])
    case class WholeFileValidationResult(errors: List[String], singleResults: List[ObjectValidationResult]) {
      def anyErrors: Boolean = errors.nonEmpty || singleResults.flatMap(_.errors).nonEmpty
    }
  end ErrorMessage

  object Counts:

    case class ObjectCounts(assetCount: Int, fileCount: Int, topLevelCount: Int)

    case class ParentWithType(parentId: Option[UUID], objectType: Type)

  end Counts

  object LambdaConfiguration:
    case class StateOutput(batchId: String, metadataPackage: URI)

    given Encoder[StateOutput] = deriveEncoder[StateOutput]

    case class Input(batchId: String, metadataPackage: URI)

    given Decoder[Input] = deriveDecoder[Input]

    case class Config() derives ConfigReader

    case class Dependencies(s3: DAS3Client[IO])
  end LambdaConfiguration
