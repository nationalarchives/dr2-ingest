# DR2 Ingest - Validate Generic Ingest Inputs

Validates the input to the `dr2-ingest` Step Function to prevent unrecoverable errors downstream. See
the [ingest docs](/docs/ingest.md) for more information.

## Input

```json
{
  "batchId": "batch",
  "metadataPackage": "s3://metadata-bucket/name/metadata.json"
}
```

The Lambda:

1. Is invoked by the `dr2-ingest` Step Function step:
2. Streams the [`metadataPackage` JSON file](#example-of-a-metadatajson-file) from the location given in the event. The
   stream processes individual objects concurrently, up to 1000 at a time.
3. For each object processed in the stream:
    1. Decode the object into a `MandatoryFields` object. This contains the fields we need in order for the ingest to
       succeed. If this fails, the errors are returned. If this is successful, the following steps are run in parallel:
        1. Update a map of `id` to `(parentId, type)` with the id, parentId and type of that object.
        2. Update a map of `parentId` to `count` where count is the number of files with that parent.
        3. Update a counts object which counts assets, files and top level folders.
        4. If the object is a File object, check it is in S3 by calling HeadObject on its location.
        5. Get the relevant JSON schema files for the type of object (from `resources/<lower_case_type>`) and validate the object against all of them.
        6. If there are errors, return the original json with a `List[String]` of errors.
4. Once all objects are processed, if there were any errors, the rest of the process is skipped, the errors are uploaded to a file in S3 and an exception is thrown with file location.
5. If there were no errors, check that the keyset of the `id` to `(parentId, type)` map contains all entries in the
   keyset of the `parentId` to `count` map. This checks that every `parentId` is also a valid id.
6. For each entry in the map of `id` to `(parentId, type)`
    1. Validate that the parent type of the object [is valid](#valid-parents)
    2. Validate that the existing parent is not found twice in the same chain. This indicates a circular dependency.
    3. Validate that for an asset, there is at least one child.
    4. Validate that there's at least one top level folder
    5. Validate that there's at least one asset.
    6. Validate that there's at least one file.
7. Accumulate the errors into one object. If there are any, write them to S3 and raise an error with the location. If
   not, return the Lambda input as its output.

## Output

If the validation succeeds, the lambda returns:

```json
{
  "batchId": "batch",
  "metadataPackage": "s3://metadata-bucket/name/metadata.json"
}
```

If the validation fails, it will upload the errors to S3.

Errors which can be found when processing each individual object are included with the original json.
Top-level errors which require the whole file to be scanned are included in the top-level errors array.
This means we don't have to hold the whole json object in memory at any point, only the errors and the id maps.

```json
{
  "errors": [
    "Asset 0f9cab98-114d-4e72-b378-695559155c45 has no children"
  ],
  "singleResults": [
    {
      "json": {
        "type": "File",
        "id": "0f9cab98-114d-4e72-b378-695559155c45",
        "name": "TestName",
        "parentId": "122980ac-2664-4965-9879-f27c524f3668",
        "representationSuffix": 1,
        "representationType": "Preservation",
        "sortOrder": 1,
        "location": "s3://bucket/key",
        "checksum_sha256": "abcd",
        "checksum_md5": "abcd",
        "title": "title"
      },
      "errors": [
        "0f9cab98-114d-4e72-b378-695559155c45 $.checksum_sha256: must be at least 64 characters long"
      ]
    }
  ]
}
```

#### Example of a metadata.json file

```json
[
  {
    "series": "ABC 123",
    "id_Code": "cite",
    "id_Cite": "cite",
    "id_URI": "https://example.com/id/court/2023/",
    "id": "4e6bac50-d80a-4c68-bd92-772ac9701f14",
    "parentId": null,
    "title": "test",
    "type": "ArchiveFolder",
    "name": "https://example.com/id/court/2023/"
  },
  {
    "originalMetadataFiles": [
      "61ac0166-ccdf-48c4-800f-29e5fba2efda"
    ],
    "description": "test",
    "id_ConsignmentReference": "test-identifier",
    "id_UpstreamSystemReference": "TEST-REFERENCE",
    "transferringBody": "test-organisation",
    "transferCompleteDatetime": "2023-10-31T13:40:54Z",
    "upstreamSystem": "TRE: FCL Parser workflow",
    "digitalAssetSource": "Born Digital",
    "digitalAssetSubtype": "FCL",
    "id_URI": "https://example.com/id/court/2023/",
    "id_NeutralCitation": "cite",
    "id": "c2e7866e-5e94-4b4e-a49f-043ad937c18a",
    "parentId": "4e6bac50-d80a-4c68-bd92-772ac9701f14",
    "title": "Test.docx",
    "type": "Asset",
    "name": "Test.docx"
  },
  {
    "id": "c7e6b27f-5778-4da8-9b83-1b64bbccbd03",
    "parentId": "c2e7866e-5e94-4b4e-a49f-043ad937c18a",
    "title": "Test",
    "type": "File",
    "name": "Test.docx",
    "sortOrder": 1,
    "location": "s3://raw-cache-bucket/53e7e334-a0bb-4dd2-ac26-0e428db56982",
    "fileSize": 15684
  },
  {
    "id": "61ac0166-ccdf-48c4-800f-29e5fba2efda",
    "parentId": "c2e7866e-5e94-4b4e-a49f-043ad937c18a",
    "title": "",
    "type": "File",
    "name": "TRE-TEST-REFERENCE-metadata.json",
    "location": "s3://raw-cache-bucket/96a07aa3-c4c5-40b2-b546-c51d2f24dce3",
    "sortOrder": 2,
    "fileSize": 215
  }
]
```

## Valid parents

| objectType    | validParentTypes                    |
|---------------|-------------------------------------|
| ArchiveFolder | ArchiveFolder                       |
| ContentFolder | ArchiveFolder, ContentFolder        |
| Asset         | ContentFolder, ArchiveFolder, Asset |
| File          | Asset                               | 

## Environment Variables

There are no Environment variables for this Lambda (everything needed is in the Lambda input).
