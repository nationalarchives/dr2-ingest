package uk.gov.nationalarchives.e2etests

import aws.sdk.kotlin.services.cloudwatchlogs.CloudWatchLogsClient
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.sfn.SfnClient
import aws.sdk.kotlin.services.sqs.SqsClient
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.cucumber.java.en.Given
import io.cucumber.java.en.Then
import io.cucumber.java.en.When
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertTrue
import uk.gov.nationalarchives.lib.IngestUtils
import java.util.*

class StepDefs {

    private val fileIds: MutableList<UUID> = mutableListOf()

    private val config: Config = ConfigFactory.load()

    private val utils = createUtils()

    private fun createUtils(): IngestUtils = runBlocking {
        val sqsClient = SqsClient.fromEnvironment()
        val s3Client = S3Client.fromEnvironment()
        val cloudWatchLogsClient = CloudWatchLogsClient.fromEnvironment()
        val dynamoClient = DynamoDbClient.fromEnvironment()
        val sfnClient = SfnClient.fromEnvironment()
        IngestUtils(sqsClient, s3Client, cloudWatchLogsClient, dynamoClient, sfnClient, config, fileIds)
    }

    @Given("An ingest with {int} files")
    fun anIngestWithFiles(numberOfFiles: Int) = runBlocking {
        utils.createFiles(numberOfFiles)
    }

    @Given("A judgment")
    fun aJudgment() = runBlocking {
        utils.createJudgment()
    }

    @When("I send a message to the judgment queue")
    fun iSendAMessageToTheJudgmentQueue() = runBlocking {
        utils.sendJudgmentMessage()
    }

    @When("I send messages to the input queue")
    fun iSendMessagesToTheInputQueue() = runBlocking {
        utils.sendTdrMessages()
    }

    @Then("I receive an ingest complete message")
    fun iReceiveAnIngestCompleteMessage() = runBlocking {
        utils.checkForIngestStatusMessages(config.getString("externalLogGroup"), 60 * 60 * 1000, "complete")
        assertTrue(fileIds.isEmpty())
    }

    @Then("I receive an ingest error message")
    fun iReceiveAnIngestErrorMessage() = runBlocking {
        utils.checkForIngestStatusMessages(config.getString("externalLogGroup"), 40 * 60 * 1000, "update")
        assertTrue(fileIds.isEmpty())
    }

    @Given("An ingest with {int} file with an empty checksum")
    fun anIngestWithFileWithAnEmptyChecksum(numberOfFiles: Int) = runBlocking {
        utils.createFiles(numberOfFiles, emptyChecksum = true)
    }

    @When("I create a batch with this file")
    fun iCreateABatchWithThisFile() = runBlocking {
        utils.createBatch()
    }


    @Given("An ingest with {int} file with an invalid checksum")
    fun anIngestWithFileWithAnInvalidChecksum(numberOfFiles: Int) = runBlocking {
        utils.createFiles(numberOfFiles, invalidChecksum = true)
    }

    @Given("An ingest with {int} file with invalid metadata")
    fun anIngestWithFileWithInvalidMetadata(numberOfFiles: Int) = runBlocking {
        utils.createFiles(numberOfFiles, invalidMetadata = true)
    }

    @Then("I receive an error in the validation queue")
    fun iReceiveAnErrorInTheValidationQueue() {
        utils.checkForValidationFailureMessages(config.getString("copyFilesLogGroup"), 40 * 60 * 1000)
    }


}