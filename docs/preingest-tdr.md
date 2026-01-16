# TDR Preingest

We are notified of records transferred to The National Archives via [Transfer Digital Records (TDR)](https://www.nationalarchives.gov.uk/information-management/manage-information/digital-records-transfer/transfer-digital-records-tdr/) via an event received to an SQS queue. This triggers the importer lambda.

The [`Preingest TDR Importer` Lambda](/python/lambdas/preingest-importer/) is responsible for unzipping
and untarring the package from TDR and for copying the file and metadata file to our raw cache bucket.
The lambda then sends a message to the aggregator queue

The [`Preingest TDR Aggregator` Lambda](/scala/lambdas/preingest-tdr-aggregator/) is responsible for batching up messages for ingest.
These are batched either based on the number of messages received in a 10-minute window or when the number of messages reaches 10000.
The batches are written to DynamoDB and the preingest step function is triggered.

The [`Preingest TDR Package Builder` Lambda](/scala/lambdas/preingest-tdr-package-builder/) is responsible for creating the DR2 ingest metadata which will be passed to the ingest process.
Once this has been created and uploaded to S3, an ingest step function is triggered.
