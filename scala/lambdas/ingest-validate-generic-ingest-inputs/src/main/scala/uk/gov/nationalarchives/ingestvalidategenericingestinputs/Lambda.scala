//package uk.gov.nationalarchives.ingestvalidategenericingestinputs
//
//import cats.effect.{IO, Resource}
//import cats.effect.unsafe.implicits.global
//import cats.implicits.*
//import com.networknt.schema.SpecVersion.VersionFlag
//import com.networknt.schema.JsonSchemaFactory
//import com.networknt.schema.InputFormat.JSON
//import fs2.interop.reactivestreams.*
//import fs2.{Chunk, Pipe, Stream, text}
//
//import org.scanamo.*
//import org.scanamo.generic.semiauto.*
//import pureconfig.ConfigReader
//import pureconfig.generic.derivation.default.*
//import ujson.*
//import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda.{Config, Dependencies, Input, StateOutput}
//import uk.gov.nationalarchives.utils.LambdaRunner
//import uk.gov.nationalarchives.DAS3Client
//import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
//
//import java.net.URI
//import java.time.Instant
//import java.time.temporal.ChronoUnit
//import java.util.UUID
//import scala.io.circe.*
//import scala.io.circe.generic.auto.*
//import scala.util.{Failure, Success, Try}
//
//class Lambda extends LambdaRunner[Input, StateOutput, Config, Dependencies] {
//  lazy private val bufferSize = 1024 * 5
//  private def getId(entry: Value) = Try(entry("id").str).getOrElse(s"missingId_${UUID.randomUUID()}")
//
//  private def parseFileFromS3(s3: DAS3Client[IO], input: Input): IO[String] =
//    for {
//      pub <- s3.download(input.metadataPackage.getHost, input.metadataPackage.getPath.drop(1))
//      s3FileString <- pub
//        .toStreamBuffered[IO](bufferSize)
//        .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
//        .through(text.utf8.decode)
//        .compile
//        .string
//    } yield s3FileString
//
//  private def checkFileIsInCorrectS3Location(s3Client: DAS3Client[IO], idAndFields: (String, Value)): IO[String] =
//    for {
//      (id, fields) <- IO.pure(idAndFields)
//      fileUri <- IO(URI.create(location))
//      res <-
//
//        fields("location") match {
//          case Success(location) =>
//            val fileUri = URI.create(location)
//            s3Client.headObject(fileUri.getHost, fileUri.getPath.drop(1))
//
//              .map(headResponse => headResponse.sdkHttpResponse()) // compare checksums if file there but checksums don't match, Failure
//
//          case Failure(e) => IO.pure()
//        }
//
//    } yield (id, res)
//
//  private def checkFileNamesHaveExtensions(fileEntries: List[Value]) =
//    fileEntries.map{ fileEntry =>
//      val id = getId(fileEntry)
//      val result = Try(fileEntry("name").str).map { name =>
//        name.split('.').toList.reverse match {
//          case ext :: _ :: _ => Success(ext)
//          case _ => Failure(new Exception(s"File name of id $id does not have an extension"))
//        }
//      }
//      id -> result
//    }
//
//
//  private def checkIfAllIdsExistAndAreUuids(metadataJson: List[UUID]): IO[Unit] = IO {
//    metadataJson.map { metadataEntry =>
//      getId(metadataEntry) -> Try(metadataEntry("id").str).map(id => Try(UUID.fromString(id)))
//    }
//  }
//
//
//  private def checkIfEntriesHaveParentIds(metadataJson: List[Value]): IO[Option[String]] = IO {
//    val fileEntries = metadataJson.filter(metadataEntry => metadataEntry("type").strOpt.contains("File"))
//    val fileIdsGroupedByParentId = fileEntries.foldLeft(Map[String, List[String]]()){
//      (idsGroupedByParentId, fileEntry) =>
//        val id = fileEntry("id").str
//        val parentId = fileEntry("parentId").str
//        val idsBelongingToParentId = idsGroupedByParentId.getOrElse(parentId, Nil)
//        idsGroupedByParentId + (parentId -> id :: idsBelongingToParentId)
//    }
//    IO.raiseWhen(fileIdsGroupedByParentId.keySet.size != 1) {
//      new Exception(s"Not all files entries are under the same parent Asset. Here is the grouping: \n $fileIdsGroupedByParentId")
//    }
//    val parentAssetId = fileIdsGroupedByParentId.keys.head
//    val assetEntries = metadataJson.filter(metadataEntry => metadataEntry("type").strOpt.contains("Asset"))
//    val numOfAssetEntries =  assetEntries.length
//    IO.raiseWhen(numOfAssetEntries != 1) {
//      new Exception(s"There are $numOfAssetEntries in this batch; there should only be 1 Asset")
//    }
//    val fileIdsBelongingToAsset = fileIdsGroupedByParentId(parentAssetId)
//    //val fileIdsAndMetadataFileIds = fileIdsBelongingToAsset(parentAssetId).groupBy{fileId => fileId ==}
//    val assetEntry = assetEntries.head
//
//
//    val originalMetadataFileIds = assetEntry("originalMetadataFiles").arr.toList.map(_.str)
//    val idsForMetadataEntries = originalMetadataFileIds.diff(fileIdsBelongingToAsset)
//
//    val originalFiles = assetEntry("originalFiles").arr.toList.map(_.str)
//    val idsForFilesEntries = fileIdsBelongingToAsset.filterNot(idsForMetadataEntries.contains)
//
//    IO.raiseWhen(originalMetadataFileIds != idsForMetadataEntries) {
//      new Exception(s"Either an id found in 'originalMetadataFiles' can not be found in the list of entries or vice-versa")
//    }
//
//    val assetWithParentId = metadataJson.filter(metadataEntry => metadataEntry("id").str == parentAssetId)
//    IO.raiseWhen(assetWithParentId.length != 1) {
//      new Exception(s"The files have a parentId of '$parentAssetId' but no Asset with this id exists")
//    }
//    //def e() =
////    val fileEntries = metadataJson.filter(metadataEntry => metadataEntry("type")).contains("File"))
//  }
//
//
//  def validateMetadataJson(metadataJsonToValidate: String) = {
//    // This creates a schema factory that will use Draft 2020-12 as the default if $schema is not specified
//    // in the schema data. If $schema is specified in the schema data then that schema dialect will be used
//    // instead and this version is ignored.
//    val jsonSchemaFactory = JsonSchemaFactory.getInstance(VersionFlag.V202012)
//    val is = getClass.getResourceAsStream("metadata-validation-schema.json")
//    val schema = jsonSchemaFactory.getSchema(is)
//
//    schema.validate(metadataJsonToValidate, JSON)
//    is.close()
//
//    val builder = SchemaValidatorsConfig.builder
//    builder.regularExpressionFactory(GraalJSRegularExpressionFactory.getInstance())
//
//    val config = builder.build
//
//    // Due to the mapping the schema will be retrieved from the classpath at classpath:schema/example-main.json.
//    // If the schema data does not specify an $id the absolute IRI of the schema location will be used as the $id.
//
////    for {
////      is <- Resource.make(IO(getClass.getResourceAsStream("/transform.xsl")))(is => IO(is.close()))
////      schema <- IO(jsonSchemaFactory.getSchema(is, config))
////      q = schema.validate(metadataJsonToValidate, InputFormat.JSON)
////    } yield ()
//
//
//
////    val assertions = , , (executionContext) => {
////
////      // By default since Draft 2019-09 the format keyword only generates annotations and not assertions
////      executionContext.getExecutionConfig.setFormatAssertionsEnabled(true)
////
////    })
//  }
//
//  override def handler: (
//      Input,
//      Config,
//      Dependencies
//  ) => IO[StateOutput] = (input, config, dependencies) =>
//    for {
//      log <- IO(log(Map("batchRef" -> input.batchId)))
//      _ <- log(s"Processing batchRef ${input.batchId}")
//      s3Client = dependencies.s3
//      bucket = input.metadataPackage.getHost
//      key =  input.metadataPackage.getPath.drop(1)
//
//      metadataJson <- parseFileFromS3(s3Client, input)
//      _ <- log("Retrieving metadata.json from s3 bucket")
//
//      validatedJson = validateMetadataJson(metadataJson)
//      metadataJsonAsUjson = read(metadataJson).arr.toList
//
//      entriesGroupedByType = metadataJson.foldLeft(Map[String, List[String]]()){
//        (entryTypesGrouped, entry) =>
//          val entryType = Try(entry("type").str).getOrElse("UnknownType")
//          val entriesBelongingToType = entryTypesGrouped.getOrElse(entryType, Nil)
//          entryTypesGrouped + (entryType -> entry :: entriesBelongingToType)
//      }
//      fileEntries = entriesGroupedByType.getOrElse("File", Nil)
//      assetEntries = entriesGroupedByType.getOrElse("Asset", Nil)
//      archivedFolderEntries = entriesGroupedByType.getOrElse("ArchivedFolder", Nil)
//      contentFolderEntries = entriesGroupedByType.getOrElse("ContentFolder", Nil)
//
//      mandatoryFieldsReportForFiles = checkMandatoryFieldsExist(fileEntries, "File")
//      checkContentOfFields(fileEntries)
//      q <- mandatoryFieldsReportForFiles.map {
//        idAndFields =>
//          idAndFields._1 -> checkFileIsInCorrectS3Location()
//      }.sequence
//
//
//
//      a = checkIfAllIdsExistAndAreUuids(metadataJson)
//      obj <- s3Client.headObject(bucket, key)
//
//      _ <- dependencies.sfn.startExecution(config.sfnArn, Output(batchRef, metadataPackage), Option(batchRef))
//
//    } yield {
//      ???// start step function OR send message to Slack and log error
//    }
//
//  override def dependencies(config: Config): IO[Dependencies] =
//    IO(Dependencies(DAS3Client[IO]()))
//}
//object Lambda {
//  case class StateOutput(batchId: String, metadataPackage: URI, archiveHierarchyFolders: List[UUID], contentFolders: List[UUID], contentAssets: List[UUID])
//  case class Input(batchId: String, metadataPackage: URI)
//  case class Config(dynamoTableName: String, discoveryApiUrl: String) derives ConfigReader
//  case class Dependencies(s3: DAS3Client[IO])
//}
