# DR2 Ingest Validate Generic Ingest Inputs

The lambda:
1. Reads the input from the step function step with this format:
```json
{
  "batchId": "batch",
  "metadataPackage": "s3://metadata-bucket/name/metadata.json"
}
```
2. Takes the location from the `metadataPackage` and downloads the [metadata.json](#example-of-a-metadatajson-file) file from the `metadataPackage` location as a String
3. Checks that the json (via JSON Schema) contains at least one `Asset` and one `File` entry and returns a list of errors (if it doesn't)
4. Checks that the json (via JSON Schema) contains at least one entry that has both a `series` and a `null` parent (as this denotes a top-level entry) and returns a list of errors (if it doesn't)
5. Splits the entries up by type (if the `type` is unknown, either because it's missing or is invalid, then it will be designated an `UnknownType` group)
6. Checks that these entries match the schema for their type:
   1. A `File` can be either a regular file or a metadata file (the `name` will end in "-metadata.json")
      1. If it's a metadata file, the title can be empty
      2. If it's a regular file, the title can't be empty
   2. For an `UnknownType`:
      1. There is a small list of properties that are common in all types that it **must** have: `id`, `parentId`,
         `title` and `type`
      2. Since it could be any type, if a field of a specific type is present, e.g. `originalFiles`, it will still be validated
   3. Archive Folders, Content Folders and Assets could contain a `series` and therefore, it must match a regex string
   4. The JSON entries are returned as a `Map`, with a `String` field name and a `Validated` (similar to an `Either`) object:
      1. The `Validated` object could be either a `Valid` ujson `Value` or an `Invalid` `NonEmptyList` of errors
      2. These `Map`s will be passed on to other methods and updated with the errors encountered
7. Filters for just `File` entries and check the S3 URI in its `location` field exists
   1. If an error occurred with the location field prior to this check (e.g. `location` is empty), it will skip this check
   2. If value can not be parsed into a URI, then it will return an error
   3. If the value is a valid URI, it will call S3
       1. If an exception is thrown, it will return an error
       2. If any status code other than a 200 occurs, then an error is returned
8. Checks that the metadata entry's file `name` has an extension and then returns an error if it does
9. Checks that the `id` of each entry is unique and returns an error (with number of occurrences) if not
10. Checks that the `id` of each entry is a valid `UUID`, and returns and error if not
11. Checks if the `parentId` for entry is correct:
    1. First, checks if the `id` has an error, if it does, it will return because it will be difficult to do the checks
    2. Check if the `parentId` is `null` (it has been converted to `None`), that could be a problem
       1. If the entry is an `ArchiveFolder`, this might be fine, so just return
       2. If the entry is a `ContentFolder`, so long as an `ArchiveFolder` (which is the parent type) doesn't exist, it's fine, so just return
       3. If the entry is an `Asset`, so long as a `ContentFolder` or `ArchiveFolder` (which are the parent types) don't exist, it's fine, so just return
       4. If the entry is a `File`, return an error, as files must have a parent (`Asset`)
       5. It's entirely possible that more than one entry of the same type has a `null` `parentId`, so long as a parent type doesn't exist, this is fine
    3. If the `parentId` is not `null`, it could either have an error on it (e.g. maybe it's empty) or have a value; if the `parentId` has an error already, then return
    4. If the `parentId` is valid, then:
       1. Check if it is the same as the `id`, if so, return an error
       2. Look for an entry that has an id that corresponds with the `parentId`, if it doesn't exist, the return an error
       3. If a parent entry can be found, check its type:
          1. A `File` entry can not be a parent of anything
          2. An `Asset` can not be the parent of an `ArchiveFolder`, `ContentFolder` or `Asset`
          3. A `ContentFolder` can not be the parent of an `ArchiveFolder` or `File`
          4. An `ArchiveFolder` can not be the parent of an `Asset` or `File`
12. Checks if an `Asset` is referencing the correct files
    1. If there are file `id`s in the JSON that have the `parentId` that matches an `Asset`'s `id` but do not appear in their expected array (either `originalFiles` or `originalMetadataFiles`) of the `Asset`, then return an error
    2. If there are files that appear in the `Asset`'s array (either `originalFiles` or `originalMetadataFiles`) but do not appear in the JSON, then return an error
13. Checks for circular dependencies in Folders.
    1. A `ContentFolder` can be the parent of another `ContentFolder` or `ArchiveFolder` and an `ArchiveFolder` can be the parent of an `ArchiveFolder`
       1. It's entirely possible that the `parentId`s (even though they make reference to `id`s that exist):
          1. Could be referencing `id`s that reference them or 
          2. Referencing `id`s that reference an `id` that references them, and so on.
          3. If this is the case, then return an error
14. Filters for entries with at least one error
15. If there are either entries with one or more errors or other general errors are present (e.g. json doesn't have at least one `File` and `Asset`)
    1. Logs a message that mentions that there are general errors (if there are any) and points to the bucket where the full results can be found
    2. Logs a message that mentions that there entry errors (if there are any) and points to the bucket where the full results can be found
    3. Logs the: `id`, fields with errors, and S3 location of the file where the full results can be found for each entry
    4. Merges all the errors into a `List[Map]`
    5. Converts the `List` to a JSON `String`
    5. Uploads the JSON `String` to a file in the same location as the `metadata.json` in S3
    6. Raises an Exception (with the same error messages that were logged) to halt the running of the step function
16. If there are no errors, and an Exception hasn't been thrown, then write the state data (which is the same as the Input) for the next step function step with this format:
```json
{
  "batchId": "batch",
  "metadataPackage": "s3://metadata-bucket/name/metadata.json"
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
    "originalFiles": [
      "c7e6b27f-5778-4da8-9b83-1b64bbccbd03"
    ],
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
## Environment Variables

There are no Environment variables for this lambda (everything needed is in the lambda Input).

[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments/blob/main/ADDLINKHERE)
