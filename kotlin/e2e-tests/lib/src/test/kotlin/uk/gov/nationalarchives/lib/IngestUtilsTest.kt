package uk.gov.nationalarchives.lib

import aws.sdk.kotlin.services.cloudwatchlogs.CloudWatchLogsClient
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.sfn.SfnClient
import aws.sdk.kotlin.services.sqs.SqsClient
import com.typesafe.config.ConfigFactory
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.SerializationException
import java.time.LocalDate
import java.util.*
import java.util.concurrent.TimeoutException
import kotlin.math.exp
import kotlin.test.*

class IngestUtilsTest {
    
    private val timeout = 100L

    @Test
    fun testIngestStatusReturnsSuccessfullyIfNoFilesToCheck() {
        messageIngestUtils(emptyList(), mutableListOf())
            .checkForIngestStatusMessages("", timeout, "complete")
    }

    @Test
    fun testIngestStatusFailsIfNoMessagesBeforeTimeout() {
        assertFailsWith<TimeoutException> {
            messageIngestUtils(emptyList(), mutableListOf(UUID.randomUUID()))
                .checkForIngestStatusMessages("", timeout, "complete")
        }
    }

    @Test
    fun testIngestStatusFailsIfInvalidJson() {
        assertFailsWith<SerializationException> {
            messageIngestUtils(listOf("invalid"), mutableListOf(UUID.randomUUID()))
                .checkForIngestStatusMessages("", timeout, "complete")
        }
    }

    @Test
    fun testIngestStatusTimeoutIfMessageIsForDifferentId() {
        val body = """{"body": 
            |{"properties": {"executionId": "", "messageType": "preserve.digital.asset.ingest.complete"}, 
            |"parameters": {"assetId": "6fd92d28-124c-40cf-a89c-13c4ff2a35ee", "status": ""}}}""".trimMargin()
        assertFailsWith<TimeoutException> {
            messageIngestUtils(listOf(body), mutableListOf(UUID.randomUUID()))
                .checkForIngestStatusMessages("", timeout, "complete")
        }
    }

    @Test
    fun testIngestStatusSuccessIfMessageIsForSameIdDifferentType() {
        val assetId = UUID.randomUUID()
        val body = """{"body": 
            |{"properties": {"executionId": "", "messageType": "preserve.digital.asset.ingest.complete"}, 
            |"parameters": {"assetId": "$assetId", "status": "Asset has been written to custodial copy disk."}}}""".trimMargin()
        assertFailsWith<TimeoutException> {
            messageIngestUtils(listOf(body), mutableListOf(assetId))
                .checkForIngestStatusMessages("", timeout, "update")
        }
    }

    @Test
    fun testIngestStatusSuccessIfMessageIsForSameIdAndCompleteType() {
        val assetId = UUID.randomUUID()
        val body = """{"body": 
            |{"properties": {"executionId": "", "messageType": "preserve.digital.asset.ingest.complete"}, 
            |"parameters": {"assetId": "$assetId", "status": "Asset has been written to custodial copy disk."}}}""".trimMargin()
        messageIngestUtils(listOf(body), mutableListOf(assetId))
            .checkForIngestStatusMessages("", 1000, "complete")
    }

    @Test
    fun testIngestStatusSuccessIfMessageIsForSameIdAndUpdateType() {
        val assetId = UUID.randomUUID()
        val body = """{"body": 
            |{"properties": {"executionId": "", "messageType": "preserve.digital.asset.ingest.update"}, 
            |"parameters": {"assetId": "$assetId", "status": "There has been an error ingesting the asset."}}}""".trimMargin()
        messageIngestUtils(listOf(body), mutableListOf(assetId))
            .checkForIngestStatusMessages("", timeout, "update")
    }

    @Test
    fun testIngestStatusTimeoutIfStatusIncorrectForUpdate() {
        val assetId = UUID.randomUUID()
        val body = """{"body": 
            |{"properties": {"executionId": "", "messageType": "preserve.digital.asset.ingest.update"}, 
            |"parameters": {"assetId": "$assetId", "status": "Invalid status"}}}""".trimMargin()
        assertFailsWith<TimeoutException> {
            messageIngestUtils(listOf(body), mutableListOf(assetId))
                .checkForIngestStatusMessages("", timeout, "update")
        }
    }

    @Test
    fun testIngestStatusTimeoutIfStatusIncorrectForComplete() {
        val assetId = UUID.randomUUID()
        val body = """{"body": 
            |{"properties": {"executionId": "", "messageType": "preserve.digital.asset.ingest.complete"}, 
            |"parameters": {"assetId": "$assetId", "status": "Invalid status"}}}""".trimMargin()
        assertFailsWith<TimeoutException> {
            messageIngestUtils(listOf(body), mutableListOf(assetId))
                .checkForIngestStatusMessages("", timeout, "complete")
        }
    }

    @Test
    fun testValidationFailureSucceedsIfSomeMessagesAreInvalidJson() {
        val assetId = UUID.randomUUID()
        val bodyList = listOf(
            """{"error": "An error", "fileId": "$assetId"}""",
            "invalidJson"
        )
        messageIngestUtils(bodyList, mutableListOf(assetId))
            .checkForValidationFailureMessages("", timeout)
    }

    @Test
    fun testValidationFailureSucceedsIfNoFilesToCheck() {
        messageIngestUtils(emptyList(), mutableListOf())
            .checkForValidationFailureMessages("", timeout)
    }

    @Test
    fun testValidationFailureTimeoutIfAssetIdDoesNotMatch() {
        val assetId = UUID.randomUUID()
        val bodyList = listOf("""{"error": "An error", "fileId": "${UUID.randomUUID()}"}""")
        assertFailsWith<TimeoutException> {
            messageIngestUtils(bodyList, mutableListOf(assetId)).checkForValidationFailureMessages("", timeout)
        }
    }

    @Test
    fun testSendTdrMessagesSendsAllFiles() {
        val returnedFiles: MutableList<UUID> = mutableListOf()
        val files = mutableListOf<UUID>(UUID.randomUUID(), UUID.randomUUID())
        runBlocking { sqsIngestUtils(returnedFiles, files).sendTdrMessages() }
        assertContentEquals(files, returnedFiles)
    }

    @Test
    fun testSendTdrMessagesSendsNoFiles() {
        val returnedFiles: MutableList<UUID> = mutableListOf()
        val files = mutableListOf<UUID>()
        runBlocking { sqsIngestUtils(returnedFiles, files).sendTdrMessages() }
        assertContentEquals(files, returnedFiles)
    }

    @Test
    fun testSendJudgmentMessagesSendsAllFiles() {
        val returnedFiles: MutableList<UUID> = mutableListOf()
        val files = mutableListOf<UUID>(UUID.randomUUID(), UUID.randomUUID())
        runBlocking { sqsJudgmentIngestUtils(returnedFiles, files).sendJudgmentMessage() }
        assertContentEquals(files, returnedFiles)
    }

    @Test
    fun testSendJudgmentMessagesSendsNoFiles() {
        val returnedFiles: MutableList<UUID> = mutableListOf()
        val files = mutableListOf<UUID>()
        runBlocking { sqsJudgmentIngestUtils(returnedFiles, files).sendJudgmentMessage() }
        assertContentEquals(files, returnedFiles)
    }

    @Test
    fun testCreateBatchWritesToDynamoAndStartsStepFunction() {
        val files = mutableListOf<UUID>(UUID.randomUUID(), UUID.randomUUID())
        val dynamoItems = mutableListOf<Map<String, String>>()
        val sfnItems = mutableListOf<JsonUtils.SFNArguments>()
        runBlocking {
            createBatchIngestUtils(dynamoItems, sfnItems, files)
                .createBatch()
        }
        assertEquals(files.size, dynamoItems.size)
        files.forEach { file ->
            val attributeMap = dynamoItems.find { it["assetId"]?.equals(file.toString()) == true }.orEmpty()
            assertEquals(file.toString(), attributeMap["assetId"])
            assertTrue(attributeMap["groupId"]?.startsWith("E2E_") == true)
            val expectedJson = """{"id":"$file","location":"s3://input-bucket/$file"}"""
            assertEquals(expectedJson, attributeMap["message"])
        }
        assertEquals(sfnItems.size, 1)
        assertTrue(sfnItems.first().batchId.startsWith("E2E_"))
        assertTrue(sfnItems.first().groupId.startsWith("E2E_"))
    }

    @Test
    fun createFilesCreatesValidFiles(): Unit = runBlocking {
        val fileContents: MutableList<UUID> = mutableListOf()
        val metadataList: MutableList<JsonUtils.TDRMetadata> = mutableListOf()
        createTdrFilesIngestUtils(fileContents, metadataList).createFiles(1)
        assertEquals(fileContents.size, 1)

        val metadata = metadataList.first()
        assertEquals(metadata.Series, "TEST 123")
        assertNotNull(metadata.UUID)
        assertNull(metadata.description)
        assertEquals(metadata.TransferringBody, "TestBody")
        assertEquals(metadata.TransferInitiatedDatetime, "2024-10-07 09:54:48")
        assertTrue(metadata.ConsignmentReference.startsWith("E2E-${LocalDate.now().year}-"))
        assertNotEquals(metadata.SHA256ServerSideChecksum, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        assertNotNull(metadata.FileReference)
    }

    @Test
    fun createFilesCreatesFileWithEmptyChecksum(): Unit = runBlocking {
        val fileContents: MutableList<UUID> = mutableListOf()
        val metadataList: MutableList<JsonUtils.TDRMetadata> = mutableListOf()
        createTdrFilesIngestUtils(fileContents, metadataList).createFiles(1, emptyChecksum = true)
        assertEquals(fileContents.size, 1)

        val metadata = metadataList.first()
        assertEquals(metadata.SHA256ServerSideChecksum, "")
    }

    @Test
    fun createFilesCreatesFileWithInvalidChecksum(): Unit = runBlocking {
        val fileContents: MutableList<UUID> = mutableListOf()
        val metadataList: MutableList<JsonUtils.TDRMetadata> = mutableListOf()
        createTdrFilesIngestUtils(fileContents, metadataList).createFiles(1, invalidChecksum = true)
        assertEquals(fileContents.size, 1)

        val metadata = metadataList.first()
        assertEquals(metadata.SHA256ServerSideChecksum, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
    }

    @Test
    fun createFilesCreatesFileWithInvalidMetadata(): Unit = runBlocking {
        val fileContents: MutableList<UUID> = mutableListOf()
        val metadataList: MutableList<JsonUtils.TDRMetadata> = mutableListOf()
        createTdrFilesIngestUtils(fileContents, metadataList).createFiles(1, invalidMetadata = true)
        assertEquals(fileContents.size, 1)

        val metadata = metadataList.first()
        assertTrue(listOf(null, "TEST123", "").contains(metadata.Series))
    }

    @Test
    fun createJudgmentCreatesAJudgmentPackage(): Unit = runBlocking {
        val expectedBytes = byteArrayOf(
            0x50, 0x4B, 0x03, 0x04,  0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x37, 0x4F,  0x5A, 0x5A, 0xDC.toByte(), 0xA7.toByte(),
            0x7C, 0x06, 0x3A, 0x08,  0x00, 0x00, 0x3A, 0x08,0x00, 0x00, 0x11, 0x00,  0x00, 0x00, 0x77, 0x6F,
            0x72, 0x64, 0x2F, 0x64,  0x6F, 0x63, 0x75, 0x6D, 0x65, 0x6E, 0x74, 0x2E,  0x78, 0x6D, 0x6C, 0x78,
            0x6D, 0x6C, 0x6E, 0x73,  0x3A, 0x77, 0x3D, 0x22, 0x68, 0x74, 0x74, 0x70,  0x3A, 0x2F, 0x2F, 0x70,
            0x75, 0x72, 0x6C, 0x2E,  0x6F, 0x63, 0x6C, 0x63, 0x2E, 0x6F, 0x72, 0x67,  0x2F, 0x6F, 0x6F, 0x78,
            0x6D, 0x6C, 0x2F, 0x77,  0x6F, 0x72, 0x64, 0x70, 0x72, 0x6F, 0x63, 0x65,  0x73, 0x73, 0x69, 0x6E,
            0x67, 0x6D, 0x6C, 0x2F,  0x6D, 0x61, 0x69, 0x6E, 0x22)
        val fileContents: MutableList<ByteArray> = mutableListOf()
        val metadataList: MutableList<JsonUtils.TREMetadata> = mutableListOf()

        createJudgmentFilesIngestUtils(fileContents, metadataList).createJudgment()

        assertContentEquals(fileContents.first(), expectedBytes)
        val metadata = metadataList.first()
        val expectedReference = metadata.parameters.TDR.UUID.toString().split("-").first()
        assertEquals(metadata.parameters.PARSER.uri, "http://example.com/id/ijkl/2025/1/doc-type/3")
        assertEquals(metadata.parameters.PARSER.cite, "cite")
        assertEquals(metadata.parameters.PARSER.name, "test")
        assertEquals(metadata.parameters.TRE.reference, expectedReference)
        assertEquals(metadata.parameters.TRE.payload.filename, "test.docx")
        assertEquals(metadata.parameters.TDR.`Document-Checksum-sha256`, "d315c315347b08cccbf38d48d54f24afa7f3d7c7740a86fdc85e2832f6367f95")
        assertEquals(metadata.parameters.TDR.`Source-Organization`, "TDR")
        assertEquals(metadata.parameters.TDR.`Internal-Sender-Identifier`, "id")
        assertEquals(metadata.parameters.TDR.`File-Reference`, expectedReference)
    }

    private fun createJudgmentFilesIngestUtils(fileContents: MutableList<ByteArray>, metadata: MutableList<JsonUtils.TREMetadata>): IngestUtils {
        return IngestUtils(
            SqsClient.builder().build(),
            AWSClients.TestJudgmentS3Client(fileContents, metadata),
            CloudWatchLogsClient.builder().build(),
            DynamoDbClient.builder().build(),
            SfnClient.builder().build(),
            ConfigFactory.load(),
            mutableListOf()
        )
    }

    private fun createTdrFilesIngestUtils(fileContents: MutableList<UUID>, metadata: MutableList<JsonUtils.TDRMetadata>): IngestUtils {
        return IngestUtils(
            SqsClient.builder().build(),
            AWSClients.TestTDRS3Client(fileContents, metadata),
            CloudWatchLogsClient.builder().build(),
            DynamoDbClient.builder().build(),
            SfnClient.builder().build(),
            ConfigFactory.load(),
            mutableListOf()
        )
    }

    private fun createBatchIngestUtils(dynamoItems: MutableList<Map<String, String>>, sfnItems: MutableList<JsonUtils.SFNArguments>, files: MutableList<UUID>): IngestUtils {
        return IngestUtils(
            SqsClient.builder().build(),
            S3Client.builder().build(),
            CloudWatchLogsClient.builder().build(),
            AWSClients.TestDynamoClient(dynamoItems),
            AWSClients.TestSfnClient(sfnItems),
            ConfigFactory.load(),
            files
        )
    }

    private fun sqsIngestUtils(returnedFiles: MutableList<UUID>, files: MutableList<UUID>): IngestUtils {
        return IngestUtils(
            AWSClients.TestTdrSqsClient(returnedFiles),
            S3Client.builder().build(),
            CloudWatchLogsClient.builder().build(),
            DynamoDbClient.builder().build(),
            SfnClient.builder().build(),
            ConfigFactory.load(),
            files
        )
    }

    private fun sqsJudgmentIngestUtils(returnedFiles: MutableList<UUID>, files: MutableList<UUID>): IngestUtils {
        return IngestUtils(
            AWSClients.TestJudgmentSqsClient(returnedFiles),
            S3Client.builder().build(),
            CloudWatchLogsClient.builder().build(),
            DynamoDbClient.builder().build(),
            SfnClient.builder().build(),
            ConfigFactory.load(),
            files
        )
    }

    private fun messageIngestUtils(messageJson: List<String>, files: MutableList<UUID>): IngestUtils {
        return IngestUtils(
            SqsClient.builder().build(),
            S3Client.builder().build(),
            AWSClients.TestCloudwatchLogsClient(messageJson),
            DynamoDbClient.builder().build(),
            SfnClient.builder().build(),
            ConfigFactory.load(),
            files
        )
    }
}