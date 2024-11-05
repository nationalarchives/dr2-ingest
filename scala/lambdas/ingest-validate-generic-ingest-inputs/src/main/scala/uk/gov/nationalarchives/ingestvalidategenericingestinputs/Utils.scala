package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.*
import cats.data.Validated
import cats.effect.IO
import cats.syntax.all.*
import fs2.interop.reactivestreams.*
import fs2.{Chunk, Stream}
import io.circe.*
import io.circe.Decoder.{AccumulatingResult, Result}
import io.circe.derivation.Configuration
import io.circe.generic.semiauto.*
import org.reactivestreams.Publisher
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Checksum
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Utils.MandatoryFields.*
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.Type.*

import java.net.URI
import java.nio.ByteBuffer
import java.time.OffsetDateTime
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
    given Encoder[SingleValidationResult] = deriveEncoder[SingleValidationResult]
    given Encoder[WholeFileValidationResult] = deriveEncoder[WholeFileValidationResult]

    case class SingleValidationError(results: List[SingleValidationResult]) extends Throwable
    case class SingleValidationResult(json: Json, errors: List[String])
    case class WholeFileValidationResult(errors: List[String], singleResults: List[SingleValidationResult]) {
      def anyErrors: Boolean = errors.nonEmpty || singleResults.flatMap(_.errors).nonEmpty
    }
  end ErrorMessage

  sealed trait MandatoryFields:
    def id: UUID

    def parentId: Option[UUID]

    def getType: Type = this match
      case _: MandatoryArchiveFolderFields => ArchiveFolder
      case _: MandatoryContentFolderFields => ContentFolder
      case _: MandatoryAssetFields         => Asset
      case _: MandatoryFileFields          => File
  end MandatoryFields

  object MandatoryFields:

    given Configuration = Configuration.default.withDefaults

    case class ObjectCounts(assetCount: Int, fileCount: Int, topLevelCount: Int)

    given Decoder[RepresentationType] = (c: HCursor) => c.as[String].map(RepresentationType.valueOf)

    given Decoder[IdField] = deriveDecoder[IdField]

    case class MandatoryArchiveFolderFields(id: UUID, name: String, parentId: Option[UUID], series: Option[String]) extends MandatoryFields

    case class MandatoryContentFolderFields(id: UUID, name: String, parentId: Option[UUID], series: Option[String]) extends MandatoryFields

    case class MandatoryAssetFields(
        id: UUID,
        parentId: Option[UUID],
        series: Option[String],
        digitalAssetSource: String,
        originalFiles: List[UUID],
        originalMetadataFiles: List[UUID],
        transferCompleteDatetime: OffsetDateTime,
        transferringBody: String,
        upstreamSystem: String
    ) extends MandatoryFields

    case class MandatoryFileFields(
        id: UUID,
        name: String,
        parentId: Option[UUID],
        representationSuffix: Int,
        representationType: RepresentationType,
        sortOrder: Int,
        checksums: List[Checksum],
        location: URI
    ) extends MandatoryFields

    given Decoder[MandatoryArchiveFolderFields] = Decoder.derivedConfigured[MandatoryArchiveFolderFields]

    given Decoder[MandatoryContentFolderFields] = Decoder.derivedConfigured[MandatoryContentFolderFields]

    case class ParentWithType(parentId: Option[UUID], objectType: Type)

    private def convertToFailFast[T](result: AccumulatingResult[T]): Either[DecodingFailure, T] = result match
      case Validated.Valid(value)    => Right(value)
      case Validated.Invalid(errors) => Left(errors.head)

    given Decoder[MandatoryAssetFields] = new Decoder[MandatoryAssetFields]:
      override def apply(c: HCursor): Result[MandatoryAssetFields] = convertToFailFast(decodeAccumulating(c))

      override def decodeAccumulating(c: HCursor): AccumulatingResult[MandatoryAssetFields] =
        (
          c.downField("id").as[UUID].toValidatedNel,
          c.downField("parentId").as[Option[UUID]].toValidatedNel,
          c.downField("series").as[Option[String]].toValidatedNel,
          c.downField("digitalAssetSource").as[String].toValidatedNel,
          c.downField("originalFiles").as[List[UUID]].toValidatedNel,
          c.downField("originalMetadataFiles").as[List[UUID]].toValidatedNel,
          c.downField("transferCompleteDatetime").as[OffsetDateTime].toValidatedNel,
          c.downField("transferringBody").as[String].toValidatedNel,
          c.downField("upstreamSystem").as[String].toValidatedNel
        ).mapN(MandatoryAssetFields.apply)

    given Decoder[MandatoryFileFields] = new Decoder[MandatoryFileFields]:
      override def apply(c: HCursor): Result[MandatoryFileFields] = convertToFailFast(decodeAccumulating(c))

      override def decodeAccumulating(c: HCursor): AccumulatingResult[MandatoryFileFields] =
        (
          c.downField("id").as[UUID].toValidatedNel,
          c.downField("name").as[String].toValidatedNel,
          c.downField("parentId").as[Option[UUID]].toValidatedNel,
          c.downField("representationSuffix").as[Int].toValidatedNel,
          c.downField("representationType").as[RepresentationType].toValidatedNel,
          c.downField("sortOrder").as[Int].toValidatedNel,
          Right {
            c.keys
              .map(_.toList)
              .getOrElse(Nil)
              .filter(_.startsWith("checksum_"))
              .flatMap { key =>
                c.downField(key).as[String].toOption.map { value =>
                  Checksum(key.drop(9), value)
                }
              }
          }.toValidatedNel,
          c.downField("location").as[URI].toValidatedNel
        ).mapN(MandatoryFileFields.apply)

    given Decoder[MandatoryFields] =
      for {
        objectType <- Decoder[String].prepare(_.downField("type"))
        mandatoryFields <- objectType match
          case "ArchiveFolder" => Decoder[MandatoryArchiveFolderFields]
          case "ContentFolder" => Decoder[MandatoryContentFolderFields]
          case "Asset"         => Decoder[MandatoryAssetFields]
          case "File"          => Decoder[MandatoryFileFields]
      } yield mandatoryFields

  end MandatoryFields

  object LambdaConfiguration:
    case class StateOutput(batchId: String, metadataPackage: URI)

    given Encoder[StateOutput] = deriveEncoder[StateOutput]

    case class Input(batchId: String, metadataPackage: URI)

    given Decoder[Input] = deriveDecoder[Input]

    case class Config() derives ConfigReader

    case class Dependencies(s3: DAS3Client[IO])
  end LambdaConfiguration
