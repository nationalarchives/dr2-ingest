package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.*
import cats.data.Validated
import cats.effect.std.AtomicCell
import cats.implicits.*
import cats.effect.IO
import cats.syntax.all.*
import com.networknt.schema.{JsonSchema, JsonSchemaFactory}
import com.networknt.schema.InputFormat.JSON
import com.networknt.schema.SpecVersion.VersionFlag
import fs2.interop.reactivestreams.*
import fs2.io.file.*
import fs2.{Chunk, Stream}
import io.circe.*
import io.circe.Decoder.{AccumulatingResult, Result}
import io.circe.derivation.Configuration
import io.circe.generic.semiauto.*
import io.circe.parser.*
import io.circe.syntax.*
import org.reactivestreams.{FlowAdapters, Publisher}
import org.typelevel.jawn.Facade
import org.typelevel.jawn.fs2.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Checksum
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.LambdaNew.{*, given}
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.Type.*
import uk.gov.nationalarchives.utils.LambdaRunner

import scala.jdk.CollectionConverters.*
import java.net.URI
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.UUID
import scala.annotation.tailrec

class Lambda extends LambdaRunner[Input, StateOutput, Config, Dependencies] {
  given Facade[Json] = io.circe.jawn.CirceSupportParser.facade

  private def updateCounts(mandatoryFields: MandatoryFields, countsCell: AtomicCell[IO, Counts]): IO[Unit] =
    val parentId = mandatoryFields.parentId

    def updateTopLevelCount(series: Option[String]) =
      val topLevelIncrement = if series.nonEmpty && parentId.isEmpty then 0 else 1
      countsCell.update(counts => counts.copy(topLevelCount = counts.topLevelCount + topLevelIncrement))

    mandatoryFields match
      case maf: MandatoryArchiveFolderFields => updateTopLevelCount(maf.series)
      case mcf: MandatoryContentFolderFields => updateTopLevelCount(mcf.series)
      case maf: MandatoryAssetFields         => updateTopLevelCount(maf.series) >> countsCell.update(counts => counts.copy(assetCount = counts.assetCount + 1))
      case mff: MandatoryFileFields          => countsCell.update(counts => counts.copy(fileCount = counts.fileCount + 1))

  private def updateParentIdCount(mandatoryFields: MandatoryFields, parentIdCountCell: AtomicCell[IO, Map[Option[UUID], Int]]): IO[Unit] =
    parentIdCountCell.update { parentIdCount =>
      val parentId = mandatoryFields.parentId
      val childCount = parentIdCount.getOrElse(parentId, 0)
      parentIdCount + (parentId -> (childCount + 1))
    }

  private def updateIdToParentType(mandatoryFields: MandatoryFields, idToParentIdTypeCell: AtomicCell[IO, Map[UUID, ParentIdType]]): IO[Unit] =
    idToParentIdTypeCell.update { idToParentIdType =>
      val newField = mandatoryFields.id -> ParentIdType(mandatoryFields.parentId, mandatoryFields.getType)
      idToParentIdType + newField
    }

  private def createSchemaMap: IO[Map[Type, List[JsonSchema]]] =
    val schemaFactory = JsonSchemaFactory.getInstance(VersionFlag.V202012)
    Type.values
    .toList
    .traverse { typeValue =>
      fs2.io.file
        .Files[IO]
        .list(Path(getClass.getResource(s"/${typeValue.toString.toLowerCase}").getPath))
        .flatMap(Files[IO].readUtf8)
        .map(schemaFactory.getSchema)
        .compile
        .toList
        .map(contents => typeValue -> contents)
    }
    .map(_.toMap)

  private def validateWholeFile(idToParentType: Map[UUID, ParentIdType], parentIdCount: Map[Option[UUID], Int], counts: Counts): List[String] = {
    val parentIdDiff = parentIdCount.keys.toSet.flatten.diff(idToParentType.keys.toSet)

    @tailrec
    def validateParentIds(id: UUID, errors: List[String] = Nil): List[String] = {
      val parentIdOpt = idToParentType.get(id).flatMap(_.parentId)
      val typeErrors = (for {
        fileType <- idToParentType.get(id).map(_.objectType)
        parentType <- parentIdOpt.flatMap(idToParentType.get).map(_.objectType)
      } yield {
        if fileType.validParent(parentType) then Nil
        else if fileType == Asset && parentIdCount.getOrElse(id.some, 0) == 0 then List(s"Asset $id has no children")
        else List(s"Parent type $parentType is not valid for file type $fileType for id $id")
      }).toList.flatten

      if parentIdOpt.isEmpty then Nil
      else if typeErrors.nonEmpty then validateParentIds(parentIdOpt.get, typeErrors ++ errors)
      else validateParentIds(parentIdOpt.get, errors)
    }

    val parentTypeErrors = idToParentType.flatMap { case (id, _) =>
      validateParentIds(id)
    }.toList
    val parentMissingErrors =
      if parentIdDiff.nonEmpty
      then List(s"Parent ids ${parentIdDiff.mkString(",")} do not exist in the json file")
      else Nil
    val missingEntryErrors =
      List(
        if counts.topLevelCount == 0 then List("There is no top level folder in this metadata file") else Nil,
        if counts.assetCount == 0 then List("There is not at least one asset in this metadata file") else Nil,
        if counts.fileCount == 0 then List("There is not at least one file in this metadata file") else Nil
      ).flatten

    parentTypeErrors ++ parentMissingErrors ++ missingEntryErrors
  }

  override def handler: (Input, Config, Dependencies) => IO[StateOutput] = (input, _, dependencies) => {

    def uploadErrors(input: Input, allValidation: WholeFileValidationResult) = {
      IO.whenA(allValidation.anyErrors) {
        val bucket = input.metadataPackage.getHost
        val resultPath = s"${input.metadataPackage.getPath}/validation-result.json"
        Stream.evals[IO, List, Byte](IO.pure(allValidation.asJson.noSpaces.getBytes.toList)).chunks.map(_.toByteBuffer).toPublisherResource.use { pub =>
          dependencies.s3.upload(bucket, resultPath, FlowAdapters.toPublisher(pub)).void
        } >> IO.raiseError(new Exception(s"Validation failed. Results are at s3://$bucket/$resultPath"))
      }
    }

    def validateIndividualObjects(
                stream: Stream[IO, Byte],
                idToParentIdTypeCell: AtomicCell[IO, Map[UUID, ParentIdType]],
                parentIdCountCell: AtomicCell[IO, Map[Option[UUID], Int]],
                countsCell: AtomicCell[IO, Counts],
                schemaMap: Map[Type, List[JsonSchema]]
    ): IO[List[SingleObjectValidationResult]] = {

      def checkObjectInS3(mandatoryFields: MandatoryFields): IO[List[String]] =
        mandatoryFields match
          case f: MandatoryFileFields =>
            dependencies.s3
              .headObject(f.location.getHost, f.location.getPath.drop(1))
              .map(_ => Nil)
              .handleError(_ => List(s"File ${f.id} can not be found in S3"))
          case _ => IO(Nil)

      def validateAgainstSchemas(mandatoryFields: MandatoryFields, json: Json): IO[List[String]] = {
        schemaMap.get(mandatoryFields.getType).toList.flatten.parTraverse { schema =>
          IO.blocking(schema.validate(json.noSpaces, JSON).asScala.map(_.getMessage))
        }.map(_.flatten)
      }

      def deserialiseAndValidate(json: Json) = decodeAccumulating[MandatoryFields](json.noSpaces)
        .fold(
          errors =>
            IO.pure(errors.toList.map(_.getMessage)),
          mandatoryFields =>
            for {
              _ <- updateIdToParentType(mandatoryFields, idToParentIdTypeCell)
              _ <- updateParentIdCount(mandatoryFields, parentIdCountCell)
              _ <- updateCounts(mandatoryFields, countsCell)
              s3Errors <- checkObjectInS3(mandatoryFields)
              schemaValidationErrors <- validateAgainstSchemas(mandatoryFields, json)
            } yield s3Errors ++ schemaValidationErrors
        )
        .map(errors => if errors.nonEmpty then SingleObjectValidationResult(json, errors).some else None )

      stream.chunks
        .unwrapJsonArray[Json]
        .parEvalMap(1000) { json =>
          deserialiseAndValidate(json)
        }
        .compile
        .toList
        .map(_.flatten)
    }
    given Monoid[Map[UUID, ParentIdType]] = Monoid.instance[Map[UUID, ParentIdType]](Map.empty, (_, second) => second)
    for {
      pub <- dependencies.s3.download(input.metadataPackage.getHost, input.metadataPackage.getPath.drop(1))
      schemaMap <- createSchemaMap
      idToParentIdTypeCell <- AtomicCell[IO].empty[Map[UUID, ParentIdType]]
      parentIdCountCell <- AtomicCell[IO].empty[Map[Option[UUID], Int]]
      countsCell <- AtomicCell[IO].of(Counts(0, 0, 0))
      singleObjectValidation <- validateIndividualObjects(pub.toByteStream, idToParentIdTypeCell, parentIdCountCell, countsCell, schemaMap)
      parentIdType <- idToParentIdTypeCell.get
      parentIdCount <- parentIdCountCell.get
      counts <- countsCell.get
      allValidation = WholeFileValidationResult(validateWholeFile(parentIdType, parentIdCount, counts), singleObjectValidation)
      _ <- uploadErrors(input, allValidation)
    } yield {
      StateOutput(input.batchId, input.metadataPackage)
    }
  }

  override def dependencies(config: Config): IO[Dependencies] = IO.pure(Dependencies(DAS3Client[IO]()))
}

object LambdaNew {

  extension (pub: Publisher[ByteBuffer])
    def toByteStream: Stream[IO, Byte] = pub
      .toStreamBuffered[IO](1024)
      .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))

  case class StateOutput(batchId: String, metadataPackage: URI)

  given Encoder[StateOutput] = deriveEncoder[StateOutput]

  case class Input(batchId: String, metadataPackage: URI)

  given Decoder[Input] = deriveDecoder[Input]

  case class Config() derives ConfigReader

  case class Dependencies(s3: DAS3Client[IO])

  given Configuration = Configuration.default.withDefaults

  given Decoder[RepresentationType] = (c: HCursor) => c.as[String].map(RepresentationType.valueOf)

  given Decoder[IdField] = deriveDecoder[IdField]

  sealed trait MandatoryFields {
    def id: UUID

    def parentId: Option[UUID]

    def getType: Type = this match
      case _: MandatoryArchiveFolderFields => ArchiveFolder
      case _: MandatoryContentFolderFields => ContentFolder
      case _: MandatoryAssetFields         => Asset
      case _: MandatoryFileFields          => File
  }

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

  case class ParentIdType(parentId: Option[UUID], objectType: Type)

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

  trait ValidationResult:
    def errors: List[String]

  given Encoder[SingleObjectValidationResult] = deriveEncoder[SingleObjectValidationResult]
  given Encoder[WholeFileValidationResult] = deriveEncoder[WholeFileValidationResult]

  case class SingleObjectValidationResult(json: Json, errors: List[String]) extends ValidationResult
  case class WholeFileValidationResult(errors: List[String], singleResults: List[SingleObjectValidationResult]) extends ValidationResult {
    def anyErrors: Boolean = errors.nonEmpty && singleResults.nonEmpty
  }

  case class Counts(assetCount: Int, fileCount: Int, topLevelCount: Int)

}
