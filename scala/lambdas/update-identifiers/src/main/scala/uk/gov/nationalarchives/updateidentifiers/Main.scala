package uk.gov.nationalarchives.updateidentifiers

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.dp.client.Entities.{Entity, IdentifierResponse}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.Identifier
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.StructuralObject
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client

import java.time.ZonedDateTime
import scala.concurrent.duration.*

object Main extends IOApp {

  private val since: ZonedDateTime = ZonedDateTime.parse("1990-01-01T00:00:09.000000000Z[Europe/London]")

  given Logger[IO] = Slf4jLogger.getLogger[IO]


  override def run(args: List[String]): IO[ExitCode] =
    Fs2Client.entityClient("https://tna.preservica.com", args.head).flatMap { client =>
      def trimIdentifiers(entity: Entity, identifiers: Seq[IdentifierResponse]): IO[Seq[Unit]] =
        identifiers.traverse { identifier =>
          IO.whenA(identifier.value.trim != identifier.value || identifier.identifierName.trim != identifier.identifierName) {
            val newIdentifier = Identifier(identifier.identifierName.trim, identifier.value.trim)
            val entityType = entity.entityType.getOrElse(StructuralObject)
            for {
              _ <- Logger[IO].info(s"Delete,${entity.ref},'${identifier.identifierName.trim}','${identifier.value.trim}'")
              _ <- client.deleteIdentifier(entity, identifier.id)
              _ <- IO.sleep(10.milliseconds)
              _ <- Logger[IO].info(s"Add,${entity.ref}'${newIdentifier.identifierName}','${newIdentifier.value}'")
              _ <- client.addIdentifierForEntity(entity.ref, entityType, newIdentifier)
            } yield ()
          }
        }

      def processEntities(entities: Seq[Entity]): IO[Seq[Unit]] = {
        entities.traverse { entity =>
          for {
            identifiers <- client.getEntityIdentifiers(entity)
            _ <- trimIdentifiers(entity, identifiers)
          } yield ()
        }
      }


      def update(start: Int): IO[Unit] = {
        for {
          updatedPage <- client.entitiesUpdatedSince(since, start)
          _ <- Logger[IO].info(s"Found ${updatedPage.length} entities starting from $start")
          _ <- processEntities(updatedPage.filter(_.entityType.contains(StructuralObject)))
          _ <- if updatedPage.isEmpty then IO.unit else update(start + 1000)
        } yield ()
      }

      update(args.last.toInt)
    }.map(_ => ExitCode.Success).handleErrorWith(err => {
      Logger[IO].error(err)(err.getMessage).map(_ => ExitCode.Error)
    })
}
