# 15. BagIt-like package for Generic Ingest

**Date:** 2023-09

## Context

In [`./0007-single-generic-ingest-function.md`](./0007-single-generic-ingest-function.md) we decided to create a single generic ingest workflow. This workflow requires a common format to be used as the source for our generic ingest functionality.

## Decision

We will use a format based on [BagIt v1.0](https://datatracker.ietf.org/doc/html/rfc8493). This format is designed to support our AWS Serverless workflows by enabling parallel processing and secure storage of payload files in S3.

We have decided to extend BagIt v1.0 as there is already precedent for using it within the department and development team. BagIt is a well known format within the digital preservation sector.

### Changes from BagIt v1.0

This document is structured to match the BagIt v1.0 specification. Changes to facilitate DR2 bags are included within the existing headings.

#### 2. Structure

- Tag directories MUST NOT be used.

#### 2.1. Required Elements

##### 2.1.1. Bag Declaration: bagit.txt

- _ENCODING_ MUST be "UTF-8"
- A "bagit.json" MUST be included, containing a JSON representation of the same data; this is for system use.

##### 2.1.2. Payload Directory: data/

- Payload files MUST NOT be organised in arbitrary subdirectory structures within the payload directory.
- Payload files MUST be named with a UUID and no file extension.

#### (New) 2.1.4. Metadata Tag File: payload-metadata.json

The “payload-metadata.json” file is a tag file that contains metadata describing each entity within the payload of the bag. The metadata within this file is intended primarily for system use. The file consists of a single JSON array with an object for each entity within the bag. As the payload directory requires a flat structure of UUID filenames, hierarchy MAY be represented within this file using the “parent” field upon an entity. To represent hierarchy, the following types MAY be used in the “type” field: “ArchiveFolder”, “ContentFolder”, “Asset”, “File”.

“ArchiveFolder” represents a folder created to manage archival hierarchy. This differs from “ContentFolder” which is used to represent a folder that is part of the transfer. “Assets” are an abstraction from “File”; one “Asset” MAY contain multiple “File”. Each “File” MUST BE identified with a UUID that is present within the payload directory, and each file within the payload directory MUST be included within “metadata.json”.

The schema for this file is below.

```json
// DRAFT
{
	"$schema": "",
	"$id": "",
	"title": "DR2 BagIt metadata.json",
	"description": "The metadata.json file within a DR2 BagIt package.",
	"type": "array",
	"items": {
		"type": "object",
		"required": ["id", "type", "name"],
		"properties": {
			"id": { "type": "string", "format": "uuid" },
			"parentId": { "type": "string", "format": "uuid" },
			"type": { "type": "string", "enum": ["ArchiveFolder", "ContentFolder", "Asset", "File"] },
			"name": { "type": "string" },
			"fileSize": { "type": "int" },
			"sortOrder": { "type": "int" }
		},
		"additionalProperties": { "type": "string" },
		"if": {
			"properties": { "type": { "const": "File" } }
		},
		"then": {
			"required": ["fileSize", "sortOrder"]
		},
		"else": {}
	}
}
```

### 2.2. Optional Elements

#### 2.2.3. Fetch File: fetch.txt

- A fetch file MUST NOT be used.

### 4. Examples

#### 4.1 Example of Basic DR2 Bag

```text
s3://<bucket/
    <bag>/
        bagit.txt
        bag-info.json
        bag-info.txt
        data/
            6bc18b07-8bbc-447a-8f7f-e655ee5ed6a3
            3206eed7-8fd6-44aa-8ee2-35462c903a15
        metadata.json
        manifest-sha256.txt
        tagmanifest-sha256.txt
```

**metadata.json**

```json
[
	{
		"id": "1a208d45-8c52-4d56-804c-1e201c616653",
		"parentId": null,
		"title": "A vs B",
		"type": "ArchiveFolder",
		"name": "https://caselaw.nationalarchives.gov.uk/id/abcd/2023/123"
	},
	{
		"id": "88b148d6-9793-49e3-9f6f-2538e23fcca8",
		"parentId": "1a208d45-8c52-4d56-804c-1e201c616653",
		"title": "A vs B",
		"type": "Asset",
		"name": null,
		"id_Code": "J/123/ABC",
		"id_Cite": "[2023] ABCD 123",
		"id_URI": "https://caselaw.nationalarchives.gov.uk/id/abcd/2023/123"
	},
	{
		"id": "741c56d4-46ef-49a7-a33a-b423d5f07cf9",
		"parentId": "88b148d6-9793-49e3-9f6f-2538e23fcca8",
		"title": "Re RB (capacity)",
		"type": "File",
		"name": "Re RB (capacity).docx",
		"sortOrder": 1,
		"fileSize": 12061
	},
	{
		"id": "2de1208e-15c3-4022-9ab2-faea06bfb0b7",
		"parentId": "88b148d6-9793-49e3-9f6f-2538e23fcca8",
		"title": "",
		"type": "File",
		"name": "TRE-TDR-2023-RMW-metadata.json",
		"sortOrder": 2,
		"fileSize": 2394
	}
]
```

## Consequences

- Our preingest workflows must produce this package to invoke the Generic Ingest workflow.
- The Mapper Lambda will be responsible for parsing this package and transforming it into the required schemas for ingest.
