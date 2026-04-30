package uk.gov.nationalarchives.lib

import aws.sdk.kotlin.services.cloudwatchlogs.CloudWatchLogsClient
import aws.sdk.kotlin.services.cloudwatchlogs.model.LiveTailSessionLogEvent
import aws.sdk.kotlin.services.cloudwatchlogs.model.StartLiveTailRequest
import aws.sdk.kotlin.services.cloudwatchlogs.model.StartLiveTailResponseStream
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.sdk.kotlin.services.sfn.SfnClient
import aws.sdk.kotlin.services.sfn.model.StartExecutionRequest
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.SendMessageRequest
import aws.smithy.kotlin.runtime.content.ByteStream
import com.typesafe.config.Config
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.takeWhile
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import uk.gov.nationalarchives.lib.JsonUtils.ExternalNotificationMessage
import uk.gov.nationalarchives.lib.JsonUtils.Parser
import uk.gov.nationalarchives.lib.JsonUtils.Payload
import uk.gov.nationalarchives.lib.JsonUtils.TDRMetadata
import uk.gov.nationalarchives.lib.JsonUtils.TDRParams
import uk.gov.nationalarchives.lib.JsonUtils.TREMetadata
import uk.gov.nationalarchives.lib.JsonUtils.TREMetadataParameters
import uk.gov.nationalarchives.lib.JsonUtils.TREParams
import uk.gov.nationalarchives.lib.JsonUtils.ValidationErrorMessage
import uk.gov.nationalarchives.lib.JsonUtils.jsonCodec
import uk.gov.nationalarchives.lib.JsonUtils.AdhocMetadata
import java.io.ByteArrayOutputStream
import java.net.URI
import java.security.MessageDigest
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.TimeoutException
import java.util.zip.GZIPOutputStream
import kotlin.random.Random

class IngestUtils(
    private val sqsClient: SqsClient,
    private val s3Client: S3Client,
    private val cloudWatchLogsClient: CloudWatchLogsClient,
    private val dynamoDbClient: DynamoDbClient,
    private val sfnClient: SfnClient,
    private val config: Config,
    private val assetIds: MutableList<UUID>
) {
    private val completeStatus = "Asset has been written to custodial copy disk."
    private val failedStatus = "There has been an error ingesting the asset."

    fun checkForValidationFailureMessages(sourceSystem: String, timeout: Long) {
        val logGroupArn = SourceSystem.fromString(sourceSystem).getCopyFilesLogGroup(config)
        streamLogs(timeout, logGroupArn) { logEvents: List<LiveTailSessionLogEvent>? ->
            logEvents?.let { events ->
                val assetIdsFromMessage = events
                    .flatMap {
                        try {
                            listOf(jsonCodec.decodeFromString<ValidationErrorMessage>(it.message!!))
                        } catch (_: Exception) {
                            emptyList()
                        }
                    }
                    .map { it.assetId }
                assetIds.removeAll(assetIdsFromMessage)
                assetIds.isEmpty()
            } ?: false
        }
    }


    fun checkForIngestStatusMessages(logGroupArn: String, timeout: Long, messageType: String) {
        val status = if (messageType == "update") failedStatus else completeStatus
        streamLogs(timeout, logGroupArn) { logEvents: List<LiveTailSessionLogEvent>? ->
            logEvents?.let { events ->
                val assetIdsFromMessage = events
                    .map { jsonCodec.decodeFromString<ExternalNotificationMessage>(it.message!!) }
                    .filter { it.body.properties.messageType == "preserve.digital.asset.ingest.$messageType" && it.body.parameters.status == status }
                    .map { it.body.parameters.assetId }
                assetIds.removeAll(assetIdsFromMessage)
                assetIds.isEmpty()
            } ?: false
        }
    }

    suspend fun createFiles(
        numberOfFiles: Int,
        sourceSystem: String = "TDR",
        emptyChecksum: Boolean = false,
        invalidMetadata: Boolean = false,
        invalidChecksum: Boolean = false
    ) {
        val invalidChecksumValue = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        val bucketName = SourceSystem.fromString(sourceSystem).getBucket(config)
        assetIds.addAll(List<UUID>(numberOfFiles) { UUID.randomUUID() })
        coroutineScope {
            assetIds.map {
                launch {
                    val checksum = if (emptyChecksum) ""
                    else if (invalidChecksum) invalidChecksumValue
                    else hash(it.toString())
                    val fileId = UUID.randomUUID()
                    uploadFileToS3(bucketName,"$it/${fileId}", ByteStream.fromString(it.toString()))
                    uploadFileToS3(bucketName,"${it}.metadata", createMetadataJson(sourceSystem, it, fileId, checksum, invalidMetadata))
                }
            }
        }
    }

    private fun idToRef(id: UUID): String = id.toString().split("-").first()

    private fun createTarGzByteStream(id: UUID, metadataBytes: ByteArray): ByteStream {
        //This identifies as a Word doc in DROID.
        val wordDocBytes = byteArrayOf(
            0x50, 0x4B, 0x03, 0x04,  0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x37, 0x4F,  0x5A, 0x5A, 0xDC.toByte(), 0xA7.toByte(),
            0x7C, 0x06, 0x3A, 0x08,  0x00, 0x00, 0x3A, 0x08,0x00, 0x00, 0x11, 0x00,  0x00, 0x00, 0x77, 0x6F,
            0x72, 0x64, 0x2F, 0x64,  0x6F, 0x63, 0x75, 0x6D, 0x65, 0x6E, 0x74, 0x2E,  0x78, 0x6D, 0x6C, 0x78,
            0x6D, 0x6C, 0x6E, 0x73,  0x3A, 0x77, 0x3D, 0x22, 0x68, 0x74, 0x74, 0x70,  0x3A, 0x2F, 0x2F, 0x70,
            0x75, 0x72, 0x6C, 0x2E,  0x6F, 0x63, 0x6C, 0x63, 0x2E, 0x6F, 0x72, 0x67,  0x2F, 0x6F, 0x6F, 0x78,
            0x6D, 0x6C, 0x2F, 0x77,  0x6F, 0x72, 0x64, 0x70, 0x72, 0x6F, 0x63, 0x65,  0x73, 0x73, 0x69, 0x6E,
            0x67, 0x6D, 0x6C, 0x2F,  0x6D, 0x61, 0x69, 0x6E, 0x22
        )
        val byteArrayOutputStream = ByteArrayOutputStream()
        val batchRef = idToRef(id)

        val filesToTar = listOf("$batchRef/test.docx" to wordDocBytes, "$batchRef/TRE-$batchRef-metadata.json" to metadataBytes)

        GZIPOutputStream(byteArrayOutputStream).use { gzipOutput ->
            TarArchiveOutputStream(gzipOutput).use { tarOutput ->
                for ((fileName, fileContent) in filesToTar) {
                    val entry = TarArchiveEntry(fileName)
                    entry.size = fileContent.size.toLong()
                    tarOutput.putArchiveEntry(entry)
                    tarOutput.write(fileContent)
                    tarOutput.closeArchiveEntry()
                }
                tarOutput.finish()
            }
        }
        return ByteStream.fromBytes(byteArrayOutputStream.toByteArray())
    }

    private fun createJudgmentMetadata(id: UUID): ByteArray {
        val parser = Parser("http://example.com/id/ijkl/2025/1/doc-type/3", "cite", "test", listOf(), listOf())
        val treParams = TREParams(idToRef(id), Payload("test.docx"))
        val checksum = "d315c315347b08cccbf38d48d54f24afa7f3d7c7740a86fdc85e2832f6367f95"
        val tdrParams = TDRParams(checksum, "TDR", "id", OffsetDateTime.now(), idToRef(id), id)
        return jsonCodec.encodeToString(TREMetadata(TREMetadataParameters(parser, treParams, tdrParams))).toByteArray()
    }

    suspend fun createJudgment() {
        val bucketName = SourceSystem.JUDGMENT.getBucket(config)
        val id = UUID.randomUUID()
        assetIds.add(id)
        val metadataBytes = createJudgmentMetadata(id)
        val tarGz = createTarGzByteStream(id, metadataBytes)
        uploadFileToS3(bucketName,"$id.tar.gz", tarGz)
    }

    suspend fun sendImportMessages(sourceSystem: String) = coroutineScope {
        val queue = SourceSystem.fromString(sourceSystem).getImporterQueue(config)
        
        assetIds.map {
            async {
                val request = SendMessageRequest {
                    queueUrl = queue
                    messageBody = buildMessageBody(sourceSystem, it)
                }
                sqsClient.sendMessage(request)
            }
        }
    }
    
    fun buildMessageBody(sourceSystem: String, id: UUID): String {
        val bucket = SourceSystem.fromString(sourceSystem).getBucket(config)
        if (sourceSystem == SourceSystem.JUDGMENT.systemName) {
            val inputParameters = JsonUtils.TREInputParameters("", idToRef(id), true, bucket, "$id.tar.gz")
            return jsonCodec.encodeToString(JsonUtils.TREInput(inputParameters))
        } else {
            return jsonCodec.encodeToString(JsonUtils.SqsInputMessage(id, bucket))
        }
    }

    suspend fun createBatch() = coroutineScope {
        val groupId = "E2E_${UUID.randomUUID()}"
        val batchId = "${groupId}_0"
        assetIds.map {
            async {
                val input = jsonCodec.encodeToString(
                    JsonUtils.AggregatorInputMessage(it, URI.create("s3://${config.getString("s3Bucket")}/$it.metadata"))
                )
                val request = PutItemRequest {
                    conditionExpression = "attribute_not_exists(assetId)"
                    tableName = config.getString("lockTable")
                    item = mapOf(
                        "assetId" to AttributeValue.S(it.toString()),
                        "groupId" to AttributeValue.S(groupId),
                        "message" to AttributeValue.S(input),
                        "createdAt" to AttributeValue.S(DateTimeFormatter.ISO_DATE_TIME.format(ZonedDateTime.now())),
                    )
                }
                dynamoDbClient.putItem(request)
            }.join()
        }
        val sfnInput = jsonCodec.encodeToString(JsonUtils.SFNArguments(groupId, batchId, 0, 2))
        val startExecutionRequest = StartExecutionRequest {
            stateMachineArn = config.getString("preingestSfnArn")
            input = sfnInput
            name = batchId
        }
        sfnClient.startExecution(startExecutionRequest)

    }

    private fun hash(body: String): String {
        val bytes = body.toByteArray()
        val md = MessageDigest.getInstance("SHA-256")
        val digest = md.digest(bytes)
        return digest.fold("") { str, it -> str + "%02x".format(it) }
    }

    private fun makeReference(length: Int): String {
        val chars = ('A'..'Z') + ('0'..'9')
        return chars.shuffled(Random).take(length).joinToString("")
    }

    private fun createMetadataJson(sourceSystem: String, assetId: UUID, fileId: UUID, checksum: String, invalidMetadata: Boolean): ByteStream {
        val thisYear = LocalDate.now().year
        fun <T> generateValue(value: T): T? = if (invalidMetadata && Random.nextBoolean()) null else value

        fun generateSeries() = listOf(null, "TEST123", "").shuffled().first()
        val series = if (invalidMetadata) generateSeries() else "TEST 123"

        val encodedMetadata = if (sourceSystem == SourceSystem.TDR.systemName) jsonCodec.encodeToString(listOf(TDRMetadata(
            series,
            generateValue(assetId),
            null,
            "TestBody",
            generateValue("2024-10-07 09:54:48"),
            "E2E-${thisYear}-${makeReference(4)}",
            "${assetId}.txt",
            checksum,
            "Z${makeReference(5)}",
            "/",
            fileId
        ))) else if (sourceSystem == SourceSystem.ADHOC.systemName) jsonCodec.encodeToString(listOf(AdhocMetadata(
            series,
            generateValue(assetId),
            UUID.randomUUID(),
            null,
            "${assetId}.txt",
            "Z${makeReference(5)}",
            "/",
            "C12345678",
            "some:file",
            "SS 1/2/34",
            checksum
        ))) else {
            throw IllegalArgumentException("Invalid source system: $sourceSystem")
        }
        return ByteStream.fromString(encodedMetadata)
    }

    private suspend fun uploadFileToS3(bucketName: String, objectKey: String, bodyStream: ByteStream) {
        val request = PutObjectRequest {
            bucket = bucketName
            key = objectKey
            body = bodyStream
        }
        s3Client.putObject(request)
    }

    private fun streamLogs(timeout: Long, logGroup: String, isComplete: (List<LiveTailSessionLogEvent>?) -> Boolean) =
        runBlocking {
            val request = StartLiveTailRequest {
                logGroupIdentifiers = listOf(logGroup)
            }

            cloudWatchLogsClient.startLiveTail(request) { response ->
                response.responseStream?.let { stream ->
                    try {
                        withTimeout(timeout) {
                            stream.takeWhile { value ->
                                when (value) {
                                    is StartLiveTailResponseStream.SessionUpdate -> {
                                        !isComplete(value.asSessionUpdate().sessionResults!!)
                                    }
                                    else -> true
                                }
                            }.collect {
                                it.asSessionUpdateOrNull()?.sessionResults?.forEach { result -> println(result.message)}
                            }
                        }
                    } catch (_: TimeoutCancellationException) {
                        throw TimeoutException("Timed out")
                    }
                }
            }
        }

    enum class SourceSystem (val systemName: String){
        TDR("TDR") {
            override fun getBucket(config: Config): String {return config.getString("s3Bucket")}
            override fun getImporterQueue(config: Config): String { return config.getString("sqsQueue") }
            override fun getCopyFilesLogGroup(config: Config): String { return config.getString("copyFilesLogGroup") }
        }, 
        JUDGMENT("Judgment") {
            override fun getBucket(config: Config): String {return config.getString("s3Bucket")}
            override fun getImporterQueue(config: Config): String { return config.getString("judgmentSqsQueue") }
            override fun getCopyFilesLogGroup(config: Config): String {
                throw NotImplementedError("getCopyFilesLogGroup not implemented for Judgment")
            }
        }, 
        ADHOC("Adhoc") {
            override fun getBucket(config: Config): String {return config.getString( "adhocBucket")}
            override fun getImporterQueue(config: Config): String { return config.getString("adhocSqsQueue") }
            override fun getCopyFilesLogGroup(config: Config): String { return config.getString("copyAdhocFilesLogGroup") }
        };
    
        abstract fun getBucket(config: Config): String
        abstract fun getImporterQueue(config: Config): String
        abstract fun getCopyFilesLogGroup(config: Config): String
        fun getExternalLogGroup(config: Config): String  {return config.getString("externalLogGroup")}
        
        companion object {
            fun fromString(systemName: String): SourceSystem {
                return entries.find { it.systemName == systemName } ?: throw IllegalArgumentException("Unknown source : $systemName")
            }
        }
    }
}
