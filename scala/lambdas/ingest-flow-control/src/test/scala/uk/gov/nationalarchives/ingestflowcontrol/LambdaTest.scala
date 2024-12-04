package uk.gov.nationalarchives.ingestflowcontrol

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import cats.effect.*
import io.circe.Decoder
import uk.gov.nationalarchives.DASSMClient
import uk.gov.nationalarchives.ingestflowcontrol.Lambda.FlowControlConfig

class LambdaTest extends AnyFlatSpec:

  def ssmClient(ref: Ref[IO, FlowControlConfig]): DASSMClient[IO] = new DASSMClient[IO]:
    override def getParameter[T](parameterName: String, withDecryption: Boolean)(using Decoder[T]): IO[T] = ref.get.map(_.asInstanceOf[T])

  // some case class validations
  "SourceSystem" should "error when the name is empty" in {
    intercept[IllegalArgumentException] {
      Lambda.SourceSystem("")
    }.getMessage should be("requirement failed: System name should not be empty")
  }

  "SourceSystem" should "error when the dedicated channels count is negative" in {
    intercept[IllegalArgumentException] {
      Lambda.SourceSystem("something", -1)
    }.getMessage should be("requirement failed: Dedicated channels should not be fewer than zero")
  }

  "SourceSystem" should "error when the probability is not between 0 and 100" in {
    intercept[IllegalArgumentException] {
      Lambda.SourceSystem("something", 5, -23)
    }.getMessage should be("requirement failed: Probability must be between 0 and 100")

    intercept[IllegalArgumentException] {
      Lambda.SourceSystem("something", 5, 123)
    }.getMessage should be("requirement failed: Probability must be between 0 and 100")
  }

  "FlowControlConfig" should "error when the source systems list is empty" in {
    intercept[IllegalArgumentException] {
      Lambda.FlowControlConfig(5, List.empty)
    }.getMessage should be("requirement failed: Source systems list cannot be empty")
  }

  "FlowControlConfig" should "error when the sum of probabilities in the flow control is not 100" in {
    intercept[IllegalArgumentException] {
      Lambda.FlowControlConfig(
        6,
        List(
          Lambda.SourceSystem("SystemOne", 2, 25),
          Lambda.SourceSystem("SystemTwo", 3, 65),
          Lambda.SourceSystem("SystemThree", 1),
          Lambda.SourceSystem("default", 0, 5)
        )
      )
    }.getMessage should be("requirement failed: The probability of all systems together should equate to 100%")
  }

  "FlowControlConfig" should "error when the dedicated channels exceed maximum concurrency" in {
    intercept[IllegalArgumentException] {
      Lambda.FlowControlConfig(
        4,
        List(
          Lambda.SourceSystem("SystemOne", 1, 25),
          Lambda.SourceSystem("SystemTwo", 2, 65),
          Lambda.SourceSystem("SystemThree", 1, 10),
          Lambda.SourceSystem("default", 2)
        )
      )
    }.getMessage should be("requirement failed: Total of dedicated channels exceed maximum concurrency")
  }

  "FlowControlConfig" should "error when there is a duplicate system name in the config" in {
    intercept[IllegalArgumentException] {
      Lambda.FlowControlConfig(
        4,
        List(
          Lambda.SourceSystem("SystemOne", 1, 25),
          Lambda.SourceSystem("SystemTwo", 0, 65),
          Lambda.SourceSystem("SystemTwo", 1, 10),
          Lambda.SourceSystem("default", 2)
        )
      )
    }.getMessage should be("requirement failed: System name must be unique")
  }

  "FlowControlConfig" should "error when there is no `default` system in the" in {
    intercept[IllegalArgumentException] {
      Lambda.FlowControlConfig(
        4,
        List(
          Lambda.SourceSystem("SystemOne", 1, 25),
          Lambda.SourceSystem("SystemTwo", 0, 65),
          Lambda.SourceSystem("SystemThree", 1, 10),
          Lambda.SourceSystem("SystemFour", 2)
        )
      )
    }.getMessage should be("requirement failed: Missing 'default' system in the configuration")
  }

  "FlowControlConfig" should "give availability of spare channels when at least one non-dedicated channel is available" in {
    val configWithSpareChannels = Lambda.FlowControlConfig(4,
      List(Lambda.SourceSystem("SystemOne", 1, 25), Lambda.SourceSystem("SystemTwo", 0, 35), Lambda.SourceSystem("SystemThree", 0, 10), Lambda.SourceSystem("default", 2, 30))
    )
    configWithSpareChannels.hasSpareChannels should be(true)
  }

  "FlowControlConfig" should "return false when dedicated channels equal the maximum concurrency" in {
    val configWithAllChannelsDedicated = Lambda.FlowControlConfig(4,
      List(Lambda.SourceSystem("SystemOne", 1, 25), Lambda.SourceSystem("SystemTwo", 1, 35), Lambda.SourceSystem("default", 2, 40))
    )
    configWithAllChannelsDedicated.hasSpareChannels should be(false)
  }

  "FlowControlConfig" should "give availability of dedicated channels when at least one dedicated channel is available" in {
    val configWithSpareChannels = Lambda.FlowControlConfig(4,
      List(Lambda.SourceSystem("SystemOne", 0, 25), Lambda.SourceSystem("SystemTwo", 0, 35), Lambda.SourceSystem("SystemThree", 0, 10), Lambda.SourceSystem("default", 1, 30))
    )
    configWithSpareChannels.hasDedicatedChannels should be(true)
  }

  "FlowControlConfig" should "return false when there are no dedicated channels for any system" in {
    val configWithAllChannelsDedicated = Lambda.FlowControlConfig(4,
      List(Lambda.SourceSystem("SystemOne", 0, 25), Lambda.SourceSystem("SystemTwo", 0, 35), Lambda.SourceSystem("default", 0, 40))
    )
    configWithAllChannelsDedicated.hasDedicatedChannels should be(false)
  }

  "buildProbabilityRangesMap" should "build a map of system name to ranges for all systems" in {
    val probabilitiesMap = new Lambda().buildProbabilityRangesMap(
      List(Lambda.SourceSystem("SystemOne", 1, 25), Lambda.SourceSystem("SystemTwo", 0, 65), Lambda.SourceSystem("default", 1, 10)),
      List.empty,
      1,
      Map.empty[String, (Int, Int)]
    )
    probabilitiesMap.size should be(3)
    probabilitiesMap("SystemOne")._1 should be(1)
    probabilitiesMap("SystemOne")._2 should be(26)
    probabilitiesMap("SystemTwo")._1 should be(26)
    probabilitiesMap("SystemTwo")._2 should be(91)
    probabilitiesMap("default")._1 should be(91)
    probabilitiesMap("default")._2 should be(101)
  }

  "buildProbabilityRangesMap" should "skip over any system mentioned in the 'skip list' when generating the map" in {
    val sourceSystems = List(
      Lambda.SourceSystem("SystemOne", 1, 25),
      Lambda.SourceSystem("SystemTwo", 0, 65),
      Lambda.SourceSystem("SystemThree", 1, 10),
      Lambda.SourceSystem("default", 2)
    )

    val probabilitiesMap = new Lambda().buildProbabilityRangesMap(sourceSystems, List("SystemTwo", "SystemThree"), 1, Map.empty[String, (Int, Int)])
    probabilitiesMap.size should be(2)
    probabilitiesMap("SystemOne")._1 should be(1)
    probabilitiesMap("SystemOne")._2 should be(26)
    probabilitiesMap("default")._1 should be(26)
    probabilitiesMap("default")._2 should be(26)
  }



// *** various specs **
// do nothing if there are no channels available
