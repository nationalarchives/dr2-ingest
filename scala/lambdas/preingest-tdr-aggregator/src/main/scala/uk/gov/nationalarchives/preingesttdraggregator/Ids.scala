package uk.gov.nationalarchives.preingesttdraggregator

import uk.gov.nationalarchives.utils.Generators

import java.util.UUID

object Ids:
  opaque type GroupId = String
  opaque type BatchId = String

  object GroupId:
    def apply(sourceSystem: String)(using Generators): GroupId = s"${sourceSystem}_${Generators().generateRandomUuid}"
    def apply(sourceSystem: String, id: UUID): GroupId = s"${sourceSystem}_$id"
  end GroupId

  object BatchId:
    def apply(groupId: GroupId, retryCount: Int = 0): BatchId = s"${groupId}_$retryCount"
  end BatchId

  extension (groupId: GroupId) def groupValue: String = groupId

  extension (batchId: BatchId) def batchValue: String = batchId

end Ids
