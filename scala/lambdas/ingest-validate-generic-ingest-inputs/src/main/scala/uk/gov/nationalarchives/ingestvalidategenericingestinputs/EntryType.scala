package uk.gov.nationalarchives.ingestvalidategenericingestinputs

object EntryType {
  sealed trait EntryTypeAndParent:
    val potentialParentId: Option[String]

  case class FileEntry(potentialParentId: Option[String]) extends EntryTypeAndParent

  case class MetadataFileEntry(potentialParentId: Option[String]) extends EntryTypeAndParent

  case class UnknownFileTypeEntry(potentialParentId: Option[String]) extends EntryTypeAndParent

  case class AssetEntry(potentialParentId: Option[String]) extends EntryTypeAndParent

  case class ArchiveFolderEntry(potentialParentId: Option[String]) extends EntryTypeAndParent

  case class ContentFolderEntry(potentialParentId: Option[String]) extends EntryTypeAndParent
}
