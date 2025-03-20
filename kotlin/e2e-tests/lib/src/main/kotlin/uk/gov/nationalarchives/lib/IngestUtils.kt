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
import uk.gov.nationalarchives.lib.JsonUtils.ExternalNotificationMessage
import uk.gov.nationalarchives.lib.JsonUtils.ValidationErrorMessage
import uk.gov.nationalarchives.lib.JsonUtils.TDRMetadata
import uk.gov.nationalarchives.lib.JsonUtils.jsonCodec
import java.net.URI
import java.security.MessageDigest
import java.time.LocalDate
import java.util.*
import java.util.concurrent.TimeoutException
import kotlin.random.Random

class IngestUtils(
    private val sqsClient: SqsClient,
    private val s3Client: S3Client,
    private val cloudWatchLogsClient: CloudWatchLogsClient,
    private val dynamoDbClient: DynamoDbClient,
    private val sfnClient: SfnClient,
    private val config: Config,
    private val files: MutableList<UUID>
) {

    private val completeStatus = "Asset has been written to custodial copy disk."
    private val failedStatus = "There has been an error ingesting the asset."

    fun checkForValidationFailureMessages(logGroupArn: String, timeout: Long) {
        streamLogs(timeout, logGroupArn) { logEvents: List<LiveTailSessionLogEvent>? ->
            logEvents?.let { events ->
                val assetIds = events
                    .flatMap {
                        try {
                            listOf(jsonCodec.decodeFromString<ValidationErrorMessage>(it.message!!))
                        } catch (e: Exception) {
                            emptyList()
                        }
                    }
                    .map { it.fileId }
                files.removeAll(assetIds)
                println(files)
                files.isEmpty()
            } ?: false
        }
    }


    fun checkForIngestStatusMessages(logGroupArn: String, timeout: Long, messageType: String) {
        val status = if (messageType == "update") failedStatus else completeStatus
        streamLogs(timeout, logGroupArn) { logEvents: List<LiveTailSessionLogEvent>? ->
            logEvents?.let { events ->
                val assetIds = events
                    .map { jsonCodec.decodeFromString<ExternalNotificationMessage>(it.message!!) }
                    .filter { it.body.properties.messageType == "preserve.digital.asset.ingest.$messageType" && it.body.parameters.status == status }
                    .map { it.body.parameters.assetId }
                files.removeAll(assetIds)
                files.isEmpty()
            } ?: false
        }
    }

    suspend fun createFiles(
        numberOfFiles: Int,
        emptyChecksum: Boolean = false,
        invalidMetadata: Boolean = false,
        invalidChecksum: Boolean = false
    ) {
        val invalidChecksumValue = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        files.addAll(List<UUID>(numberOfFiles) { UUID.randomUUID() })
        files.forEach { println(it) }
        coroutineScope {
            files.map {
                launch {
                    val checksum = if (emptyChecksum) {
                        ""
                    } else if (invalidChecksum) {
                        invalidChecksumValue
                    } else {
                        hash(it.toString())
                    }
                    createFile(it.toString(), ByteStream.fromString(it.toString()))
                    createFile("${it}.metadata", createMetadataJson(it, checksum, invalidMetadata))
                }
            }
        }
    }

    suspend fun sendMessages() = coroutineScope {
        val bucket = config.getString("s3Bucket")
        files.map {
            async {
                val request = SendMessageRequest {
                    queueUrl = config.getString("sqsQueue")!!
                    messageBody = jsonCodec.encodeToString(JsonUtils.SqsInputMessage(it, bucket))
                }
                sqsClient.sendMessage(request)
            }
        }
    }

    suspend fun createBatch() = coroutineScope {
        val groupId = "E2E_${UUID.randomUUID()}"
        val batchId = "${groupId}_0"
        files.map {
            async {
                val input = jsonCodec.encodeToString(
                    JsonUtils.AggregatorInputMessage(
                        it,
                        URI.create("s3://${config.getString("s3Bucket")}/$it")
                    )
                )
                val request = PutItemRequest {
                    conditionExpression = "attribute_not_exists(assetId)"
                    tableName = config.getString("lockTable")
                    item = mapOf(
                        "assetId" to AttributeValue.S(it.toString()),
                        "groupId" to AttributeValue.S(groupId),
                        "message" to AttributeValue.S(input)
                    )
                }
                dynamoDbClient.putItem(request)
            }.join()
        }
        val sfnInput = jsonCodec.encodeToString(JsonUtils.SFNArguments(groupId, batchId, 0, 0))
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

    private fun createMetadataJson(id: UUID, checksum: String, invalidMetadata: Boolean): ByteStream {
        val thisYear = LocalDate.now().year
        fun <T> generateValue(ifPresent: T) = if (invalidMetadata && Random.nextBoolean()) {
            null
        } else {
            ifPresent
        }

        fun generateSeries() = listOf(null, "TEST123", "").shuffled().first()
        val series = if (invalidMetadata) {
            generateSeries()
        } else "TEST 123"
        val metadata = TDRMetadata(
            series,
            generateValue(id),
            null,
            "TestBody",
            generateValue("2024-10-07 09:54:48"),
            "E2E-${thisYear}-${makeReference(4)}",
            "${id}.txt",
            checksum,
            "Z${makeReference(5)}"
        )
        return ByteStream.fromString(jsonCodec.encodeToString(metadata))
    }

    private suspend fun createFile(objectKey: String, bodyStream: ByteStream) {
        val bucketName = config.getString("s3Bucket")!!
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
}