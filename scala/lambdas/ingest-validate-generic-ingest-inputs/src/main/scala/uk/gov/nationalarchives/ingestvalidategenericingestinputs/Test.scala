import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global
//import cats.implicits.*
import com.networknt.schema.SpecVersion.VersionFlag
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.InputFormat.JSON
//import uk.gov.nationalarchives.DAS3Client
import scala.jdk.CollectionConverters.*

object Hello extends App {
  val jsonSchemaFactory = JsonSchemaFactory.getInstance(VersionFlag.V202012)
//      val builder = SchemaValidatorsConfig.builder
//      builder.regularExpressionFactory(GraalJSRegularExpressionFactory.getInstance())


  val is = getClass.getResourceAsStream("metadata-validation-schema.json")
  val schema = jsonSchemaFactory.getSchema(is)
  val input =
  """[{"series":"A 167","id_Code":"hello","id_URI":"zzz","id":"h","parentId":null,"title":null,"type":"ContentFolder","name":"zzz"},{"originalFiles":["00bc4004-a0dd-426f-abaa-6b846391fd8f"],"originalMetadataFiles":["640f48e1-c587-4440-8962-4a75b05038b5"],"transferringBody":"vlah","transferCompleteDatetime":"2024-08-05T13:58:41Z","upstreamSystem":"aaa","digitalAssetSource":"Born Digital","digitalAssetSubtype":"FCL","id_BornDigitalRef":"ZHPL","id_ConsignmentReference":"sfsdf","id_RecordID":"f5d6c25c-e586-4e63-a45b-9c175b095c48","id":"f5d6c25c-e586-4e63-a45b-9c175b095c48","parentId":"64ad1ab1-cb26-4ae5-b50b-c1926af688ff","title":"fhfghfgh","type":"Asset","name":"f5d6c25c-e586-4e63-a45b-9c175b095c48"}, {"id":"00bc4004-a0dd-426f-abaa-6b846391fd8f","parentId":"f5d6c25c-e586-4e63-a45b-9c175b095c48","title":"ttt","type":"File","name":"xcvxcvx","sortOrder":1,"fileSize":15613,"representationType":"Preservation","representationSuffix":1,"location":"s3://ghfgh/00bc4004-a0dd-426f-abaa-6b846391fd8f","checksum_sha256":"111"}, {"id":"00bc4004-a0dd-426f-abaa-6b846391fd8f","parentId":"f5d6c25c-e586-4e63-a45b-9c175b095c48","title":"ttt","type":"File","name":"xcvxcvx","sortOrder":1,"fileSize":15613,"representationType":"Preservation","representationSuffix":1,"location":"s3://ghfgh/00bc4004-a0dd-426f-abaa-6b846391fd8f","checksum_sha256":"111"}, {"id":"640f48e1-c587-4440-8962-4a75b05038b5","parentId":"f5d6c25c-e586-4e63-a45b-9c175b095c48","title":"hjkhjk","type":"File","name":"jjj","sortOrder":2,"fileSize":1159,"representationType":"Preservation","representationSuffix":1,"location":"s3://ghfgh/640f48e1-c587-4440-8962-4a75b05038b5","checksum_sha256":"222"}]""".stripMargin
  val results = schema.validate(input, JSON).asScala.toList
//  print("\n\n\n", results.length)
//  results.foreach(r => print("\n\n\n\n", r))
//  is.close()

  val resource = for {
    is <- Resource.make(IO(getClass.getResourceAsStream("metadata-validation-schema.json")))(is => IO(is.close()))
  } yield is
  
  val e = resource.use { is =>
    print("\nHere1")
    for {
      schema <- IO(jsonSchemaFactory.getSchema(is))
      q = print("\nHere2")
      result <- IO(schema.validate(input, JSON))
      w = print("\nHere3")
    } yield result
  }
  e.unsafeRunSync()
//  val q = DAS3Client[IO]().headObject("", "")
//    .map{headResponse =>
//      val r = headResponse.sdkHttpResponse()
//      print(r)
//      print(r.statusCode())
//      r
//    } // compare checksums if file there but checksums don't match, Failure
//  println(q.unsafeRunSync())
}