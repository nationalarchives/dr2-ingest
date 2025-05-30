package uk.gov.nationalarchives.ingestworkflowmonitor

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.TableDrivenPropertyChecks
import uk.gov.nationalarchives.ingestworkflowmonitor.Lambda.{Config, StateOutput}
import uk.gov.nationalarchives.ingestworkflowmonitor.testUtils.ExternalServicesTestUtils

class LambdaTest extends ExternalServicesTestUtils with TableDrivenPropertyChecks:
  val config: Config = Config("")

  forAll(runningStatuses) { (apiStatus, normalisedStatus) =>
    "handler" should s"pass a '$normalisedStatus' 'state', 'mappedId', 0 succeededAssetId, 0 failedAssetIds, 0 duplicatedAssetIds" +
      s"to the OutputStream if the status returned from the API is $apiStatus" in {
        val dependencies = createDependencies(
          IO.pure(Seq(defaultMonitor.copy(status = apiStatus)))
        )
        val stateData = new Lambda().handler(input, config, dependencies).unsafeRunSync()

        stateData.status should be(normalisedStatus)
        stateData.mappedId should be("a69099e5236501684d415d70b9e8ec7d")
      }
  }

  forAll(succeededStatuses) { (apiStatus, normalisedStatus) =>
    "handler" should s"pass a '$normalisedStatus' 'state', 'mappedId', 1 succeededAssetId, 2 failedAssetIds, 0 duplicatedAssetIds" +
      s"to the OutputStream if the status returned from the API is $apiStatus" in {
        val dependencies = createDependencies(
          IO.pure(Seq(defaultMonitor.copy(status = apiStatus)))
        )
        val stateData = new Lambda().handler(input, config, dependencies).unsafeRunSync()

        stateData.status should be(normalisedStatus)
        stateData.mappedId should be("a69099e5236501684d415d70b9e8ec7d")
      }
  }

  forAll(failedStatuses) { (apiStatus, normalisedStatus) =>
    "handler" should s"pass a '$normalisedStatus' 'state', 'mappedId', 0 succeededAssetId, 3 failedAssetIds, 0 duplicatedAssetIds" +
      s"to the OutputStream if the status returned from the API is $apiStatus" in {
        val dependencies = createDependencies(
          IO.pure(Seq(defaultMonitor.copy(status = apiStatus)))
        )
        val stateData = new Lambda().handler(input, config, dependencies).unsafeRunSync()

        stateData.status should be(normalisedStatus)
        stateData.mappedId should be("a69099e5236501684d415d70b9e8ec7d")
      }
  }

  "handler" should "return an exception if the API returns one" in {
    val exception = IO.raiseError(new Exception("API has encountered an issue when calling 'getMonitors'"))
    val dependencies = createDependencies(exception)

    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }

    ex.getMessage should equal("API has encountered an issue when calling 'getMonitors'")
  }

  "handler" should s"return an exception if the API returns a status that's unexpected" in {
    val dependencies = createDependencies(
      IO.pure(Seq(defaultMonitor.copy(status = "InvalidStatus")))
    )

    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }

    ex.getMessage should equal("'InvalidStatus' is an unexpected status!")
  }

  "handler" should s"return an exception if the API returns 0 Monitors" in {
    val dependencies = createDependencies(
      IO.pure(Nil)
    )

    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }

    ex.getMessage should equal("'Ingest' Monitor was not found!")
  }
