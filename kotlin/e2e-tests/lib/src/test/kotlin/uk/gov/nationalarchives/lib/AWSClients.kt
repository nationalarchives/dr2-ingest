package uk.gov.nationalarchives.lib

import aws.sdk.kotlin.services.cloudwatchlogs.CloudWatchLogsClient
import aws.sdk.kotlin.services.cloudwatchlogs.model.*
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.BatchGetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.BatchGetItemResponse
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.DeleteMarkerEntry
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.GetObjectResponse
import aws.sdk.kotlin.services.s3.model.GetObjectTaggingRequest
import aws.sdk.kotlin.services.s3.model.GetObjectTaggingResponse
import aws.sdk.kotlin.services.s3.model.ListObjectVersionsRequest
import aws.sdk.kotlin.services.s3.model.ListObjectVersionsResponse
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.sdk.kotlin.services.s3.model.PutObjectResponse
import aws.sdk.kotlin.services.s3.model.Tag
import aws.sdk.kotlin.services.sfn.SfnClient
import aws.sdk.kotlin.services.sfn.model.DescribeExecutionRequest
import aws.sdk.kotlin.services.sfn.model.DescribeExecutionResponse
import aws.sdk.kotlin.services.sfn.model.ExecutionStatus
import aws.sdk.kotlin.services.sfn.model.StartExecutionRequest
import aws.sdk.kotlin.services.sfn.model.StartExecutionResponse
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.SendMessageRequest
import aws.sdk.kotlin.services.sqs.model.SendMessageResponse
import aws.smithy.kotlin.runtime.content.ByteStream
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
import java.net.URI
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

    class TestTdrSqsClient(private val messageList: MutableList<JsonUtils.SqsInputMessage>, delegate: SqsClient = SqsClient.builder().build()) : SqsClient by delegate {
        override suspend fun sendMessage(input: SendMessageRequest): SendMessageResponse {
            val sqsMessage = jsonCodec.decodeFromString<JsonUtils.SqsInputMessage>(input.messageBody!!)
            messageList.add(sqsMessage)
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

        override suspend fun batchGetItem(input: BatchGetItemRequest): BatchGetItemResponse {
            val responseItems: List<Map<String, AttributeValue>> = dynamoList.map { item -> item.map { it.key to AttributeValue.S(it.value) }.toMap() }
            return BatchGetItemResponse {
                responses = mapOf("lock-table" to responseItems)
            }
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

    class TestIngestSfnClient(val sfnInput: JsonUtils.SfnInput, val executionStatus: ExecutionStatus, delegate: SfnClient = SfnClient.builder().build()): SfnClient by delegate {
        override suspend fun describeExecution(input: DescribeExecutionRequest): DescribeExecutionResponse {
            return DescribeExecutionResponse {
                executionArn = "test"
                this.input = jsonCodec.encodeToString<JsonUtils.SfnInput>(sfnInput)
                startDate = Instant.now()
                stateMachineArn = "test"
                status = executionStatus
            }
        }
    }

    class TestMetadataS3Client(val s3Files: Map<URI, String>, val s3Tags: Map<URI, Tag>, val deleteMarkerResponse: List<String>, delegate: S3Client = S3Client.builder().build()): S3Client by delegate {
        override suspend fun listObjectVersions(input: ListObjectVersionsRequest): ListObjectVersionsResponse {
            return ListObjectVersionsResponse {
                deleteMarkers = deleteMarkerResponse.map { DeleteMarkerEntry { key = it } }
            }
        }

        override suspend fun <T> getObject(input: GetObjectRequest, block: suspend (GetObjectResponse) -> T): T {
            return block(GetObjectResponse {
                body = ByteStream.fromString(s3Files[URI.create("s3://${input.bucket}/${input.key}")]!!)
            })
        }

        override suspend fun getObjectTagging(input: GetObjectTaggingRequest): GetObjectTaggingResponse {
            val tags = if (s3Tags.isEmpty()) listOf() else listOf(s3Tags[URI.create("s3://${input.bucket}/${input.key}")]!!)
            return GetObjectTaggingResponse {
                tagSet = tags
            }

        }
    }

    class TestTDRS3Client(private val fileContents: MutableList<UUID>, private val metadata: MutableList<JsonUtils.TDRMetadata>, delegate: S3Client = S3Client.builder().build()): S3Client by delegate {
        override suspend fun putObject(input: PutObjectRequest): PutObjectResponse {
            if (input.key?.endsWith("metadata") == true) {
                input.body?.decodeToString()?.let { metadata.addAll(jsonCodec.decodeFromString<List<JsonUtils.TDRMetadata>>(it)) }
            } else {
                input.body?.decodeToString()?.let { fileContents.add(UUID.fromString(it.substring(0, 36))) }
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