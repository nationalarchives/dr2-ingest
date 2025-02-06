package uk.gov.nationalarchives.updateidentifiers

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.dp.client.Entities.{Entity, IdentifierResponse}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.Identifier
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.StructuralObject
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client

import java.time.ZonedDateTime
import java.util.UUID
import scala.concurrent.duration.*
import scala.io.Source

object Main extends IOApp {

  private val since: ZonedDateTime = ZonedDateTime.parse("1990-01-01T00:00:09.000000000Z[Europe/London]")

  given Logger[IO] = Slf4jLogger.getLogger[IO]


  override def run(args: List[String]): IO[ExitCode] =
    Fs2Client.entityClient("https://tna.preservica.com", args(1)).flatMap { client =>

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

      case class SourceId(id: UUID, value: String)

      def findSourceIds(entities: Seq[Entity]): IO[Seq[Unit]] = {
        entities.traverse { entity =>
          client.getEntityIdentifiers(entity).flatMap { identifiers =>
            if identifiers.nonEmpty then
              identifiers.traverse(id => Logger[IO].info(s"${entity.ref},${id.identifierName},${id.value}"))
            else
              Seq(entity.ref.toString).traverse(s => Logger[IO].info(s))
          }
        }.map(_.flatten)
      }

      def getDuplicateSourceIds(start: Int, existingIds: List[UUID] = Nil): IO[Unit] = {
        for {
          updatedPage <- client.entitiesUpdatedSince(since, start)
          _ <- Logger[IO].info(s"Found ${updatedPage.length} entities starting from $start")
          nextEntities = updatedPage.filter(e => e.entityType.contains(StructuralObject) && !e.deleted && !existingIds.contains(e.ref))
          sourceIds <- findSourceIds(nextEntities)
          result <- if updatedPage.isEmpty then IO.unit else getDuplicateSourceIds(start + 1000, existingIds ++ nextEntities.map(_.ref))
        } yield result
      }

      def update(start: Int): IO[Unit] =
        for {
          updatedPage <- client.entitiesUpdatedSince(since, start)
          _ <- Logger[IO].info(s"Found ${updatedPage.length} entities starting from $start")
          _ <- processEntities(updatedPage.filter(_.entityType.contains(StructuralObject)))
          _ <- if updatedPage.isEmpty then IO.unit else update(start + 1000)
        } yield ()


      def deleteSourceIdIfDuplicate() = {
        val identifier = Identifier("ReviewDuplicates", "DR2-2057")
        for {
          entitiesMap <- client.entitiesPerIdentifier(Seq(identifier))
          entities = entitiesMap.getOrElse(identifier, Nil)
          _ <- Logger[IO].info(s"${entities.length} found")
          _ <- entities.traverse { entity =>
            client.getEntityIdentifiers(entity).flatMap { identifiers =>
              val sourceId = identifiers.find(_.identifierName == "SourceID")
              IO.whenA(sourceId.isDefined) {
                Logger[IO].info(s"Deleting source ID for ${entity.ref}") >>
                  client.deleteIdentifier(entity, sourceId.get.id)
              }
            }
          }
        } yield ()
      }

      def tagEmptyDuplicates(filePath: String): IO[List[Unit]] = {
        Resource.fromAutoCloseable(IO(Source.fromFile(filePath))).use { source =>
          source.getLines().toList.traverse { line =>
            val identifier = Identifier("SourceID", line)
            for {
              entityMap <- client.entitiesPerIdentifier(Seq(identifier))
              entities = entityMap.getOrElse(identifier, Nil)
              _ <- Logger[IO].info(s"Found ${entities.map(_.ref).mkString(",")}")
              children <- entities.traverse { entity =>
                client.getChildren(entity).flatMap { children =>
                  Logger[IO].info(s"Found children ${children.map(_.ref).mkString(",")}") >>
                    IO.whenA(children.isEmpty) {
                      Logger[IO].info(s"Adding ReviewDuplicates identifier to ${entity.ref}") >>
                        client.addIdentifierForEntity(entity.ref, StructuralObject, Identifier("ReviewDuplicates", "DR2-2057")).void
                    }
                }
              }
            } yield ()
          }
        }
      }

      if args.head == "d" then
        getDuplicateSourceIds(args.last.toInt)
      else if args.head == "u" then
        update(args.last.toInt)
      else if args.head == "t" then
        tagEmptyDuplicates(args.last)
      else
        deleteSourceIdIfDuplicate()
    }.map(_ => ExitCode.Success).handleErrorWith(err => {
      Logger[IO].error(err)(err.getMessage).map(_ => ExitCode.Error)
    })


}
