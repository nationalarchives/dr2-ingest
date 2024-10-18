# 14. Generic Ingest Architecture

**Date:** 2024-09

## Context

We've made some decisions that influence the design of our generic ingest workflow but need to design the implementation.

## Decision

![Diagram of ingest workflow](/docs/images/adr/0014/ingest-diagram.png)

Ingesting to the Preservation System (Preservica) is orchestrated by a single AWS Step Function. Our process expects a DR2 Bag package to be available within our `<env>-dr2-ingest-raw-cache` bucket and the Step Function to be executed, referencing the prefix this package exists within. Our process will then: load this data into a DynamoDB table, modify entities in the Preservation System using the REST API, and assemble a package for ingest into the Preservation System.

The ingest process has been designed to abstract or limit the points where our business logic and Preservation System-specific logic intersect. Logic specific to the Preservation System is limited to the Upsert, Asset OPEX Creator, Folder OPEX Creator, Parent Folder OPEX Creator, Workflow, and Reconcile Lambda functions. This logic has been further abstracted through the development of a [Preservica Client library](https://github.com/nationalarchives/dr2-preservica-client).

When a new package is ready for ingest, the calling application, one of our pre-Ingest handlers, executes a new Step Function execution, passing in: the location of our DR2 Bag, an ingest batch identifier, and the dept/series if known.

### Mapper Lambda

The Mapper Lambda loads the data from the DR2 BagIt, combining the fields in the supplied bag-info.json and metadata.json files, creating an item for each entry in metadata.json within our DynamoDB table. When the ingest process was designed, we believed a requirement of the New Preservation System was to hold the catalogue metadata as happened in the DRI system that preceded it, this is no longer the case but the design still allows for the ingesting of arbitrary metadata fields by passing through all fields from the bag-info.json and metadata.json files; where the same field exists in both source files, the metadata.json file has priority. parentPath is a special field created by this Lambda; to enable efficient querying of our DynamoDB table, this field is constructed as a concatenated string of the complete hierarchy, up to the root entity within our DR2 Bag.

The Mapper Lambda is also responsible for creating our archive hierarchy by adding the department and series folders to our representation; where the incoming package has these fields populated, the Lambda will use the department and series to request the title and description from Discovery, based upon the reference. In the example below, the Lambda has looked up the values for `J` and `J 347` within Discovery.

The state after the Mapper Lambda has run will be a populated DynamoDB table with an item for each folder, asset, and file within the ingest package as well as the department and series folders. The Mapper Lambda returns the input event with 3 additional arrays listing the IDs of some item types within our system: ArchiveFolder, ContentFolder, and Asset. File types are not listed, as these are not required to orchestrate any downstream processing.

```json
{
	"batchId": "TST-2023-2HH",
	"s3Bucket": "intg-dr2-ingest-raw-cache",
	"s3Prefix": "TST-2023-2HH/",
	"archiveHierarchyFolders": [
		"ccbadede-6c1c-42f4-bdfe-a03bd032b291",
		"7b60998b-9987-4917-99f0-09a79d107585",
		"3d772b61-59b9-4f7f-8752-4a5e5fd216ae"
	],
	"contentFolders": [],
	"contentAssets": ["77a67676-320c-4208-865b-b595e58d0962"]
}
```

**dr2-ingest-files DynamoDB Table**
|id|batchId|checksum_sha256|description|digitalAssetSource|digitalAssetSubtype|fileExtension|fileSize|id_Cite|id_Code|id_ConsignmentReference|id_NeutralCitation|id_UpstreamSystemReference|id_URI|name|originalFiles|originalMetadataFiles|parentPath|sortOrder|title|transferCompleteDatetime|transferringBody|type|upstreamSystem|
|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
3d772b61-59b9-4f7f-8752-4a5e5fd216ae|TST-2023-2HH||Records of the Supreme Court of Judicature and related courts...||||||J|||||J|||||Records of the Supreme Court of Judicature and related courts|||ArchiveFolder||
7b60998b-9987-4917-99f0-09a79d107585|TST-2023-2HH||This series contains judgments from the Civil Division...||||||J 347|||||J 347|||3d772b61-59b9-4f7f-8752-4a5e5fd216ae ||Supreme Court of Judicature: Court of Appeal: Judgments|||ArchiveFolder||
ccbadede-6c1c-42f4-bdfe-a03bd032b291|TST-2023-2HH|||||||[2023] COURT 1234|[2023] COURT 1234||||https://caselaw.nationalarchives.gov.uk/id/court/2023/1234|https://caselaw.nationalarchives.gov.uk/id/court/2023/1234|||3d772b61-59b9-4f7f-8752-4a5e5fd216ae/7b60998b-9987-4917-99f0-09a79d107585||A vs B|||ArchiveFolder||
77a67676-320c-4208-865b-b595e58d0962|TST-2023-2HH||A vs B|Born Digital|FCL|||||TST-2023-2HH|[2023] COURT 1234|[2023] COURT 1234|https://caselaw.nationalarchives.gov.uk/id/court/2023/1234|A vs B.docx|"[{""S"":""a5b1059b-2b6d-44d8-8652-249567c6bd26""}]"|"[{""S"":""6a5d3861-4bc2-475a-b047-1ebaf81012f8""}]"|3d772b61-59b9-4f7f-8752-4a5e5fd216ae/7b60998b-9987-4917-99f0-09a79d107585/ccbadede-6c1c-42f4-bdfe-a03bd032b291||A vs B.docx|2023-12-15T14:12:02Z|Courts|Asset|TRE: FCL Parser workflow|
6a5d3861-4bc2-475a-b047-1ebaf81012f8|TST-2023-2HH|ff4af36637811394ae2372667113e69ae33175ae40a17fd1f772a6eb4f2a0080||||json|1145|||||||TST-2023-2HH-metadata.json|||3d772b61-59b9-4f7f-8752-4a5e5fd216ae/7b60998b-9987-4917-99f0-09a79d107585/ccbadede-6c1c-42f4-bdfe-a03bd032b291/77a67676-320c-4208-865b-b595e58d0962|2||||File||
a5b1059b-2b6d-44d8-8652-249567c6bd26|TST-2023-2HH|d3a05d9f44967ad42faa87bf6f8e7a4b7a6938f110306ee9f844669316f41a1b||||docx|15643|||||||A vs B.docx|||3d772b61-59b9-4f7f-8752-4a5e5fd216ae/7b60998b-9987-4917-99f0-09a79d107585/ccbadede-6c1c-42f4-bdfe-a03bd032b291/77a67676-320c-4208-865b-b595e58d0962|1|A vs B|||File||

### Upsert Archive Folders Lambda

This Lambda is responsible for creating or updating certain folders within the Preservation System. This step is required as the default behaviour of the ingest mechanism is to place the full package within a nominated folder, ingesting as a child of the root node is not possible within the Preservation System. We also have a requirement to update some folder metadata on subsequent ingests, for example, if the Discovery title or description of a dept/series changes, we’d want to update this within the Preservation System. Our `ArchiveFolder` type is used to represent folders that require handling this way, whereas `ContentFolder` is used to represent folders transferred to us which we expect will not require further updates.

Given a list of `ArchiveFolder` identifiers, the Lambda function reads the metadata for these from our DynamoDB table. The Lambda works top-down, attempting to find a folder in the Preservation System where the SourceID identifier matches the name in our DynamoDB table. If the Lambda fails to find a folder in the Preservation System it will create one, setting the `SourceID` from the `name` field, the `description` from the `description` field, and the `title` from either `title` or `name`.

If the folder does exist, the Lambda will update the existing entity instead of creating a new one. To avoid spurious audit history within the Preservation System, the Lambda checks for differences before applying an update. The `title` field is only updated if the new ingest also contains a `title` value, if the new ingest only contains `name` this field is not updated.

Both when creating or updating a folder, identifiers are passed through into the Preservation System’s native Identifier feature. Any field prefixed with `id_`, will create or update the identifier of that type. E.g. `id_Code` will become the `Code` identifier within the Preservation System. This has been implemented as the values of identifiers can be dependent on upstream system - court documents have a `Cite` identifier which other document types do not, court documents also exist in a “case folder” with a custom `Code` identifier.

### Asset OPEX Creator Lambda

This Lambda function is responsible for generating a PAX and OPEX representation of each Asset type within our ingest package. The Lambda builds this within our staging bucket under a new prefix, `opex/<executionId>/`. As our DR2 BagIt representation has fewer levels of hierarchy than the Preservation System, we’ve hardcoded some duplication into this process to create the additional levels; each File within our package generates a CO with a single bitstream. The name of our files ([which are UUIDs](./0009-uuid-object-keys.md)) become the title of the CO entity.

The Lambda is run within the MAP step within our Step Function which allows us to parallelise the processing. Each Lambda is given an asset `id` within our ingest package which it then uses to fetch the metadata from our DynamoDB table. The Lambda also fetches the child files by querying for items in our DynamoDB table with a `parentPath` equal to the current asset’s `parentPath + '/' + id`. The Lambda then copies the files it finds into our new OPEX representation, using the `parentPath` attribute to place the files in the correct location for our OPEX hierarchy. Once the files have been copied, this Lambda creates the `.xip` and the `.pax.opex` XML documents to describe the PAX package.

Insertion of Asset metadata is completed here; we have the same identifier passthrough logic as in the Upsert Lambda and create our Source metadata fragment within our `.pax.opex` XML document.

### Folder OPEX Creator Lambda

Once we’ve created our PAX packages and `.pax.opex` files, we can create our `.opex` files that describe each folder within our package. This step needs to happen after our Asset OPEX Creator has run as we need to include the filesize of the `.pax.opex` files within our folder `.opex` files.

Similar to our Asset OPEX Creator, this Lambda is run in a MAP state but this time passed an `id` for an `ArchiveFolder` or `ContentFolder` entity within our DynamoDB table. Both types are processed in the same way, with the exception that `ArchiveFolder` adds the `<opex:SourceID>` element to the document with the `name` value from our table; within the Preservation System this merges the new folder with the folder we created/found when the Upsert Lambda ran.

### Parent Folder OPEX Creator Lambda

Within the Preservation System, we’ve enabled the feature `Require folder manifests` which only allows an ingest to proceed if all subfolders and files are included within the `.opex` manifests. This reduces the potential for a race condition, where the system begins ingesting whilst we’re still writing packages and enforces a check that the OPEX package we’ve supplied is complete. Unfortunately, it also requires that the top level folder (our root node) also have an `.opex` manifest; this is a folder that doesn’t get created within the Preservation System but only exists to hold our ingest package for the ingest workflow. As mentioned, our OPEX Creator Lambdas write to `opex/<executionId>` which means we need to create a the `.opex` manifest file with the `executionId` within this location i.e. `opex/<executionId>/<executionId>.opex` file. As this folder isn’t represented in our package and therefore also not in our DynamoDB table, we’ve implemented a hack to create this file by listing the contents of our S3 prefix within our staging bucket to find the direct children required for the OPEX manifest.

### DataSync: Copy to Preservica

This is an AWS service that we call to copy the OPEX package we’ve created in our staging bucket to Preservica’s ingest bucket.

### Start Workflow Lambda

This function starts the ingest workflow within Preservica, passing the location of the OPEX package’s root in their bucket. This Lambda returns a `correlationId` to help us find the workflow in Preservica.

### Workflow Monitor Lambda

This function polls Preservica to confirm the workflow status. We do this as we need to wait for the workflow to complete before continuing our with ingest process. It returns a few different statuses, which we map to either: Running, Succeeded or Failed.

```scala
 Map(
  "Running" -> "Running",
  "Pending" -> "Running",
  "Suspended" -> "Succeeded",
  "Recoverable" -> "Succeeded",
  "Succeeded" -> "Succeeded",
  "Failed" -> "Failed"
)
```

The Lambda returns, the `monitorStatus`, `mappedId` (for the purpose of calling other endpoints) as well as the assets that have succeeded, been skipped, or failed.

### Reconcile Lambda

This Lambda is responsible for ensuring that the assets ingested into Preservica are identical to the assets in our source. The Lambda is run within the MAP step within our Step Function which allows us to parallelise the processing. Like the Asset OPEX Creator Lambda, it takes an `id` of an asset, fetches the item and children from DynamoDB, then processes - comparing the checksum and title of each file.

We will use the Step Function to publish notifications once the reconcile step has completed successfully for each asset.
