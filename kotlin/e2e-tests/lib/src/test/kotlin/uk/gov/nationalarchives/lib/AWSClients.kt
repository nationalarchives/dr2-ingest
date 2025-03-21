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
import aws.smithy.kotlin.runtime.time.Instant
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import uk.gov.nationalarchives.lib.JsonUtils.jsonCodec
import java.util.*

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

    class TestSqsClient(private val filesList: MutableList<UUID>, delegate: SqsClient = SqsClient.builder().build()) : SqsClient by delegate {
        override suspend fun sendMessage(input: SendMessageRequest): SendMessageResponse {
            val fileId = jsonCodec.decodeFromString<JsonUtils.SqsInputMessage>(input.messageBody!!).fileId
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

    class TestS3Client(private val fileContents: MutableList<UUID>, private val metadata: MutableList<JsonUtils.TDRMetadata>, delegate: S3Client = S3Client.builder().build()): S3Client by delegate {
        override suspend fun putObject(input: PutObjectRequest): PutObjectResponse {
            if (input.key?.endsWith("metadata") == true) {
                input.body?.decodeToString()?.let { metadata.add(jsonCodec.decodeFromString(it)) }
            } else {
                input.body?.decodeToString()?.let { fileContents.add(UUID.fromString(it)) }
            }
            return PutObjectResponse {}
        }
    }
}