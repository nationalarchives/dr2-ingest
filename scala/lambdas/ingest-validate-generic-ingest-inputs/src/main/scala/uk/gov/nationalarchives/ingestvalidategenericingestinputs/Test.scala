import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global

import cats.implicits.*
import com.networknt.schema.SpecVersion.VersionFlag
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.InputFormat.JSON
//import com.networknt.schema.ValidationMessage
//import uk.gov.nationalarchives.DAS3Client
import scala.jdk.CollectionConverters.*
import java.io.InputStream

object Hello extends App {
  val jsonSchemaFactory = JsonSchemaFactory.getInstance(VersionFlag.V202012)
//      val builder = SchemaValidatorsConfig.builder
//      builder.regularExpressionFactory(GraalJSRegularExpressionFactory.getInstance())

//  val is = getClass.getResourceAsStream("metadata-validation-schema.json")
//  val schema = jsonSchemaFactory.getSchema(is)
//  val input =
//
//  val results = schema.validate(input, JSON).asScala.toList
//  IO.println("\n\n\n", results.length)
//  results.foreach(r => IO.println("\n\n\n\n", r))
//  is.close()

  val resource = for {
    is <- Resource.make(IO(getClass.getResourceAsStream("/metadata-validation-schemas/at-least-one-asset-and-file-validation-schema.json")))(is => IO(is.close()))
  } yield is

  val e = resource.use(is => IO(getResults(is))).flatMap { d =>
//      IO.println("\n\n\n\n\ngetCode", d.asScala.toList.headOption.map(_.getCode))
//      IO.println("\n\n\n\n\ngetType", d.asScala.toList.headOption.map(_.getType))
    IO.println("\n\n\n\n\ngetArguments", d.asScala.toList.headOption.map(_.getArguments))
    IO.println("\n\n\n\n\ngetError", d.asScala.toList.headOption.map(_.getError)) // useful
//      IO.println("\n\n\n\n\ngetDetails", d.asScala.toList.headOption.map(_.getDetails))
//      IO.println("\n\n\n\n\ngetEvaluationPath", d.asScala.toList.headOption.map(_.getEvaluationPath))
    IO.println("\n\n\n\n\ngetInstanceLocation", d.asScala.toList.headOption.map(_.getInstanceLocation)) // useful
    IO.println("\n\n\n\n\ngetInstanceNode", d.asScala.toList.headOption.map(_.getInstanceNode)) // useful useful
//      IO.println("\n\n\n\n\ngetMessage", d.asScala.toList.headOption.map(_.getMessage)) // useful useful
//      IO.println("\n\n\n\n\ngetMessageKey", d.asScala.toList.headOption.map(_.getMessageKey))
////      IO.println("\n\n\n\n\ngetProperty", d.asScala.toList.headOption.map(_.getProperty))
//      IO.println("\n\n\n\n\ngetSchemaNode", d.asScala.toList.headOption.map(_.getSchemaNode))
    IO.println(
      "\n\n\n\n\nd",
      d.asScala.toList.headOption.map { q =>
        q.getType.toString +
          "\n\n\n getEvaluationPath " + q.getEvaluationPath.toString +
          "\n\n\n getProperty " + Option(q.getProperty) +
          "\n\n\ngetInstanceNode " + q.getInstanceNode.toString +
          "\n\n\n getInstanceLocation " + q.getInstanceLocation.toString +
          "\n\n\n getMessage " + q.getMessage.toString

      }
    )
  }

  def getResults(is: InputStream) = {
    val input = """[{"series":"A 167", "id_Code":"hello","id_URI":"zzz","id":"h","parentId":null,"title":"null","type":"ContentFolder","name":"zzz"}]""".stripMargin
    val schema = JsonSchemaFactory.getInstance(VersionFlag.V202012).getSchema(is)
    val d = schema.validate(input, JSON)

    d
  }
  e.unsafeRunSync()
//  val q = DAS3Client[IO]().headObject("", "")
//    .map{headResponse =>
//      val r = headResponse.sdkHttpResponse()
//      IO.println(r)
//      IO.println(r.statusCode())
//      r
//    } // compare checksums if file there but checksums don't match, Failure
//  println(q.unsafeRunSync())
}
