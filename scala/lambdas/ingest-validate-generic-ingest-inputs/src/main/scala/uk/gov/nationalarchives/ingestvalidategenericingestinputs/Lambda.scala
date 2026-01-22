package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.*
import cats.effect.IO
import cats.effect.std.AtomicCell
import cats.implicits.*
import com.networknt.schema.InputFormat.JSON
import com.networknt.schema.{Schema, SchemaRegistry, SpecificationVersion}
import fs2.Stream
import fs2.io.file.*
import fs2.io.file as fs2File
import io.circe.*
import io.circe.parser.*
import io.circe.syntax.*
import org.reactivestreams.FlowAdapters
import org.typelevel.jawn.Facade
import org.typelevel.jawn.fs2.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Utils.ErrorMessage.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Utils.LambdaConfiguration.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Utils.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Utils.Counts.*
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.Type.*
import uk.gov.nationalarchives.utils.LambdaRunner

import java.util.UUID
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*

class Lambda extends LambdaRunner[Input, StateOutput, Config, Dependencies] {
  given Facade[Json] = io.circe.jawn.CirceSupportParser.facade

  private def updateCounts(metadataObject: MetadataObject, countsCell: AtomicCell[IO, ObjectCounts]): IO[Unit] =
    val parentId = metadataObject.parentId

    def updateTopLevelCount(series: Option[String]) =
      val topLevelIncrement = if series.nonEmpty && parentId.isEmpty then 1 else 0
      countsCell.update(counts => counts.copy(topLevelCount = counts.topLevelCount + topLevelIncrement))

    metadataObject match
      case afmo: ArchiveFolderMetadataObject => updateTopLevelCount(afmo.series)
      case cfmo: ContentFolderMetadataObject => updateTopLevelCount(cfmo.series)
      case amo: AssetMetadataObject          => countsCell.update(counts => counts.copy(assetCount = counts.assetCount + 1))
      case fmo: FileMetadataObject           => countsCell.update(counts => counts.copy(fileCount = counts.fileCount + 1))

  private def updateChildCount(metadataObject: MetadataObject, childCountCell: AtomicCell[IO, Map[Option[UUID], Int]]): IO[Unit] =
    childCountCell.update { parentIdCount =>
      val parentId = metadataObject.parentId
      val childCount = parentIdCount.getOrElse(parentId, 0)
      parentIdCount + (parentId -> (childCount + 1))
    }

  private def updateIdToParentType(metadataObject: MetadataObject, idToParentIdTypeCell: AtomicCell[IO, Map[UUID, ParentWithType]]): IO[Unit] =
    idToParentIdTypeCell.update { idToParentIdType =>
      val newField = metadataObject.id -> ParentWithType(metadataObject.parentId, metadataObject.getType)
      idToParentIdType + newField
    }

  private def createSchemaMap: IO[Map[Type, List[Schema]]] =
    val registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
    Type.values.toList
      .traverse { typeValue =>
        fs2File
          .Files[IO]
          .list(Path(getClass.getResource(s"/${typeValue.toString.toLowerCase}").getPath))
          .flatMap(Files[IO].readUtf8)
          .map(registry.getSchema)
          .compile
          .toList
          .map(contents => typeValue -> contents)
      }
      .map(_.toMap)

  private def validateWholeFile(idToParentType: Map[UUID, ParentWithType], parentIdCount: Map[Option[UUID], Int], counts: ObjectCounts): List[String] = {
    val parentIdDiff = parentIdCount.keys.toSet.flatten.diff(idToParentType.keys.toSet)

    @tailrec
    def validateParentIds(id: UUID, errors: List[ErrorMessage] = Nil, existingParents: List[UUID] = Nil): List[ErrorMessage] = {
      val parentIdOpt = idToParentType.get(id).flatMap(_.parentId)
      val fileTypeOpt = idToParentType.get(id).map(_.objectType)
      val parentTypeOpt = parentIdOpt.flatMap(idToParentType.get).map(_.objectType)
      def errorOrNil(condition: Boolean, errorMessage: ErrorMessage) = if condition then List(errorMessage) else Nil
      val typeErrors =
        errorOrNil(!fileTypeOpt.exists(_.validParent(parentTypeOpt)), ParentTypeInvalid(parentTypeOpt, fileTypeOpt, id)) ++
          errorOrNil(parentIdOpt.isDefined && existingParents.containsSlice(parentIdOpt.toList), CircularDependency(id, existingParents)) ++
          errorOrNil(fileTypeOpt.contains(Asset) && parentIdCount.getOrElse(id.some, 0) == 0, NoAssetChildren(id))

      if parentIdOpt.isEmpty then errors
      else if typeErrors.collect { case cd: CircularDependency => cd }.nonEmpty then errors ++ typeErrors // Exit early to prevent infinite loop
      else validateParentIds(parentIdOpt.get, typeErrors ++ errors, existingParents ++ parentIdOpt.toList)
    }

    val parentTypeErrors = idToParentType.keys.flatMap(id => validateParentIds(id)).toList

    val parentMissingErrors =
      if parentIdDiff.nonEmpty
      then parentIdDiff.map(MissingParent.apply)
      else Nil
    val missingEntryErrors =
      List(
        if counts.topLevelCount == 0 then List(NoTopLevelFolder) else Nil,
        if counts.assetCount == 0 then List(NoAsset) else Nil,
        if counts.fileCount == 0 then List(NoFile) else Nil
      ).flatten

    (parentTypeErrors ++ parentMissingErrors ++ missingEntryErrors).map(_.show)
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

    def validateIndividualObjects(stream: Stream[IO, Byte], schemaMap: Map[Type, List[Schema]])(
        idToParentCell: AtomicCell[IO, Map[UUID, ParentWithType]],
        parentCountCell: AtomicCell[IO, Map[Option[UUID], Int]],
        objectCountsCell: AtomicCell[IO, ObjectCounts]
    ): IO[List[ObjectValidationResult]] = {

      def checkObjectInS3(metadataObject: MetadataObject): IO[List[String]] =
        metadataObject match
          case f: FileMetadataObject =>
            dependencies.s3
              .headObject(f.location.getHost, f.location.getPath.drop(1))
              .map(_ => Nil)
              .handleError(_ => List(s"File ${f.id} can not be found in S3 at location ${f.location}"))
          case _ => IO.pure(Nil)

      def validateAgainstSchemas(metadataObject: MetadataObject, json: Json): IO[List[String]] = {
        schemaMap
          .get(metadataObject.getType)
          .toList
          .flatten
          .parTraverse { schema =>
            IO.blocking(schema.validate(json.noSpaces, JSON).asScala.map(result => s"${metadataObject.id} ${result.getMessage}"))
          }
          .map(_.flatten)
      }

      def validateSingleObject(json: Json) = decodeAccumulating[MetadataObject](json.noSpaces)
        .fold(
          errors => IO.pure(errors.toList.map(_.getMessage)),
          mandatoryFields =>
            for {
              updateIdParentFiber <- updateIdToParentType(mandatoryFields, idToParentCell).start
              updateParentCountFiber <- updateChildCount(mandatoryFields, parentCountCell).start
              updateCountsFiber <- updateCounts(mandatoryFields, objectCountsCell).start
              s3ErrorsFiber <- checkObjectInS3(mandatoryFields).start
              schemaValidationErrorsFiber <- validateAgainstSchemas(mandatoryFields, json).start
              _ <- List(updateIdParentFiber, updateParentCountFiber, updateCountsFiber).traverse(_.join)
              s3ErrorsOutcome <- s3ErrorsFiber.join
              schemaValidationOutcome <- schemaValidationErrorsFiber.join
              s3Errors <- s3ErrorsOutcome.embedError
              schemaValidationErrors <- schemaValidationOutcome.embedError
            } yield s3Errors ++ schemaValidationErrors
        )
        .map(errors => if errors.nonEmpty then ObjectValidationResult(json, errors).some else None)

      stream.chunks
        .unwrapJsonArray[Json]
        .parEvalMap(1000) { json =>
          validateSingleObject(json)
        }
        .compile
        .toList
        .map(_.flatten)
    }
    given Monoid[Map[UUID, ParentWithType]] = Monoid.instance[Map[UUID, ParentWithType]](Map.empty, (_, second) => second)
    for {
      pub <- dependencies.s3.download(input.metadataPackage.getHost, input.metadataPackage.getPath.drop(1))
      schemaMap <- createSchemaMap
      idToParentIdTypeCell <- AtomicCell[IO].empty[Map[UUID, ParentWithType]]
      parentIdCountCell <- AtomicCell[IO].empty[Map[Option[UUID], Int]]
      countsCell <- AtomicCell[IO].of(ObjectCounts(0, 0, 0))
      singleObjectValidation <- validateIndividualObjects(pub.toByteStream, schemaMap)(idToParentIdTypeCell, parentIdCountCell, countsCell)
      _ <- IO.whenA(singleObjectValidation.nonEmpty)(uploadErrors(input, WholeFileValidationResult(Nil, singleObjectValidation)))
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
