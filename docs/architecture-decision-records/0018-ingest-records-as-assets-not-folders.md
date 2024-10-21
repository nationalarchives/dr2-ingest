# 18. Ingest records as assets, not folders

**Date:** 2024-06-17

## Context

When ingesting to the Preservation System, we have a choice to represent the record as an Asset or a Folder. Many archives choose to represent records as folders within their Catalogue and Preservation System, removing the need to describe individual objects within a collection.

## Decision

We will represent records as assets within the Preservation System; this decision has been made for the following reasons.

- Digital Asset Management vs Archive Catalogue. Although we are an archive, our requirements for access drive our end to end system (Transfer through to Access) to be a digital asset manager as opposed to an archive catalogue. We must handle the preservation of individual digital objects and make these available to the presentation systems, it is up to these systems to decide how to use the objects.

- Enabling Migrations. The Preservation System in use today, Preservica, migrates files within a Content Object. Whilst an Information Object (asset) can have multiple Content Objects, each of these are migrated independently. If we used Structural Objects (folders) to represent records the Preservation System would be unable to migrate files spread across IOs or COs.

- Split Catalogue. By using the Catalogue Service to only describe records to a specific granularity, then using the Preservation Service to describe multiple objects below this - each of which could have complex metadata - we are splitting the catalogue between 2 systems.

- Increased Complexity. Representing a record as a folder allows records to contain complex file hierarchies which we must then describe to other systems that require the digital objects and handle the edge cases of partially failed ingest, exports, etc. This increases the complexity of the Preservation Service.

- Legacy Systems. Many other customers of Preservica have migrated from their V4 data model, which required records be represented as DeliverableUnits; each DeliverableUnit could contain a complex hierarchy of DeliverableUnits and Files. We are starting afresh with Preservica’s V6 data model that does not implement this requirement.

## Consequences

While this decision reduces the complexity required of the Preservation Service, it increases the complexity to be implemented within the Catalogue Service.

The Preservation Service will support records with multiple files in a flat hierarchy and records with multiple representations - e.g. Google documents transferred as both `.docx` and `.pdf`.

Whilst not implemented at this time, the Preservation Service could support files with multiple bitstreams in a flat structure - e.g. 3D models or database exports. For complex file transfers, where a file is made of many files in a structured hierarchy, the Preservation Service will be required to fall back to archive container formats, `.tar` or `.zip`, and migrate these manually.

As digital files can be recursive, an embedded file containing an embedded file, the Preservation Service will be unable to migrate the full tree as one record. The Preservation System will need to work in tandem with the Catalogue Service to create new records for extracted files if we are to migrate these.
