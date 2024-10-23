# 9. UUID filenames/object keys

**Date:** 2023-10-17

## Context

The Digital Records Repository will be responsible for: ingesting, storing, and providing access to documents transferred to The National Archives from other government departments. Under the [Freedom of Information Act 2000](https://www.legislation.gov.uk/ukpga/2000/36/contents), records can be “closed” (exempt from public access); dependent upon the record the content or the metadata can be closed.

The design of the Digital Records Repository must account for closed records, ensuring that the data is protected at all times. This presents an issue as our cloud object storage, S3, does not encrypt object keys and the Preservation System stores objects in S3 using the ingest filename within the key.

## Decision

We will abstract filenames from the underlying storage, using UUIDs to identify objects and holding metadata only within encrypted objects or encrypted database tables.

## Consequences

Digital Records Repository will be required to transform ingest packages before writing the contents to our storage. This requirement will drive the design of our custom schema package for ingest.

When building OPEX packages for ingesting into the Preservation System, all folders and filenames will need to be abstracted, with the real metadata being contained within the `.opex` metadata files. The bitstream held within the Preservation System will not have a human-readable name as this goes directly into the S3 object key. Instead, the name will be a UUID and the original file extension; the file extension is required during the "Characterise" step of ingest to identify the file type. This will affect end users within the Preservation System's user interface as the default filename when downloading an object is the bitstream name. Searching within the system will also be affected as the hyphen within the UUID will be treated as a space by the indexer; we could remove the hyphens to mitigate this.
