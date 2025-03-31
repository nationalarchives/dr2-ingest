package uk.gov.nationalarchives.lib

import aws.sdk.kotlin.services.cloudwatchlogs.CloudWatchLogsClient
import aws.sdk.kotlin.services.cloudwatchlogs.model.*
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.sdk.kotlin.services.s3.model.PutObjectResponse
import aws.sdk.kotlin.services.sfn.SfnClient
import aws.sdk.kotlin.services.sfn.model.StartExecutionRequest
import aws.sdk.kotlin.services.sfn.model.StartExecutionResponse
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.SendMessageRequest
import aws.sdk.kotlin.services.sqs.model.SendMessageResponse
import aws.smithy.kotlin.runtime.content.decodeToString
import aws.smithy.kotlin.runtime.content.toByteArray
import aws.smithy.kotlin.runtime.time.Instant
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import uk.gov.nationalarchives.lib.JsonUtils.jsonCodec
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.*
import java.util.zip.GZIPInputStream

object AWSClients {

    class TestCloudwatchLogsClient(
        private val messageJsonList: List<String>,
        delegate: CloudWatchLogsClient = CloudWatchLogsClient.builder().build()
    ) : CloudWatchLogsClient by delegate {
        override suspend fun <T> startLiveTail(
            input: StartLiveTailRequest,
            block: suspend (StartLiveTailResponse) -> T
        ): T {
            val startLiveTailResponse =
                if (messageJsonList.isEmpty()) {
                    val liveTailSessionUpdate = LiveTailSessionUpdate {
                        sessionResults = emptyList()
                    }
                    val sessionUpdate = StartLiveTailResponseStream.SessionUpdate(liveTailSessionUpdate)
                    StartLiveTailResponse {
                        responseStream = flow {
                            while (true) {
                                delay(10)
                                emit(sessionUpdate)
                            }
                        }
                    }
                } else {
                    val liveTailSessionLogEvents = messageJsonList.map { LiveTailSessionLogEvent { message = it } }
                    val emptyUpdate = LiveTailSessionUpdate { sessionResults = emptyList() }
                    val liveTailSessionUpdate = LiveTailSessionUpdate {
                        sessionResults = liveTailSessionLogEvents
                    }
                    val sessionUpdate = StartLiveTailResponseStream.SessionUpdate(liveTailSessionUpdate)
                    val emptySessionUpdate = StartLiveTailResponseStream.SessionUpdate(emptyUpdate)
                    StartLiveTailResponse {
                        responseStream = flow {
                            emit(emptySessionUpdate)
                            while (true) {
                                delay(10)
                                emit(sessionUpdate)
                            }
                        }

                    }
                }
            return block.invoke(startLiveTailResponse)
        }
    }

    class TestTdrSqsClient(private val filesList: MutableList<UUID>, delegate: SqsClient = SqsClient.builder().build()) : SqsClient by delegate {
        override suspend fun sendMessage(input: SendMessageRequest): SendMessageResponse {
            val fileId = jsonCodec.decodeFromString<JsonUtils.SqsInputMessage>(input.messageBody!!).fileId
            filesList.add(fileId)
            return SendMessageResponse {}
        }
    }

    class TestJudgmentSqsClient(private val filesList: MutableList<String>, delegate: SqsClient = SqsClient.builder().build()) : SqsClient by delegate {
        override suspend fun sendMessage(input: SendMessageRequest): SendMessageResponse {
            val fileId = jsonCodec.decodeFromString<JsonUtils.TREInput>(input.messageBody!!).parameters.reference
            filesList.add(fileId)
            return SendMessageResponse {}
        }
    }

    class TestDynamoClient(private val dynamoList: MutableList<Map<String, String>>, delegate: DynamoDbClient = DynamoDbClient.builder().build()): DynamoDbClient by delegate {
        override suspend fun putItem(input: PutItemRequest): PutItemResponse {
            dynamoList.add(input.item?.mapValues { it.value.asS() }.orEmpty())
            return PutItemResponse {}
        }
    }

    class TestSfnClient(private val sfnList: MutableList<JsonUtils.SFNArguments>, delegate: SfnClient = SfnClient.builder().build()): SfnClient by delegate {
        override suspend fun startExecution(input: StartExecutionRequest): StartExecutionResponse {
            sfnList.add(jsonCodec.decodeFromString<JsonUtils.SFNArguments>(input.input.orEmpty()))
            return StartExecutionResponse {
                executionArn = "test"
                startDate = Instant.now()
            }
        }
    }

    class TestTDRS3Client(private val fileContents: MutableList<UUID>, private val metadata: MutableList<JsonUtils.TDRMetadata>, delegate: S3Client = S3Client.builder().build()): S3Client by delegate {
        override suspend fun putObject(input: PutObjectRequest): PutObjectResponse {
            if (input.key?.endsWith("metadata") == true) {
                input.body?.decodeToString()?.let { metadata.add(jsonCodec.decodeFromString(it)) }
            } else {
                input.body?.decodeToString()?.let { fileContents.add(UUID.fromString(it)) }
            }
            return PutObjectResponse {}
        }
    }

    class TestJudgmentS3Client(private val fileContents: MutableList<ByteArray>, private val metadataList: MutableList<JsonUtils.TREMetadata>, delegate: S3Client = S3Client.builder().build()): S3Client by delegate {
        override suspend fun putObject(input: PutObjectRequest): PutObjectResponse {
            withContext(Dispatchers.IO) {
                GZIPInputStream(ByteArrayInputStream(input.body?.toByteArray() ?: ByteArray(0))).use { gzipInput ->
                    TarArchiveInputStream(gzipInput).use { tarInput ->
                        var entry: TarArchiveEntry?
                        val buffer = ByteArray(8192) // Buffer for reading

                        while (tarInput.nextEntry.also { entry = it } != null) {
                            if (entry!!.isDirectory) continue // Skip directories

                            val outputStream = ByteArrayOutputStream()
                            var bytesRead: Int
                            while (tarInput.read(buffer).also { bytesRead = it } != -1) {
                                outputStream.write(buffer, 0, bytesRead)
                            }
                            if (entry!!.name.contains("metadata")) {
                                val metadata =
                                    jsonCodec.decodeFromString<JsonUtils.TREMetadata>(outputStream.toString("utf-8"))
                                metadataList.add(metadata)
                            } else {
                                fileContents.add(outputStream.toByteArray())
                            }
                        }
                    }
                }
            }
            return PutObjectResponse {}
        }
    }
}