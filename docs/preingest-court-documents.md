# Court Document Preingest

Court Documents are delivered to DR2 from the [Transformation Engine (TRE)](https://github.com/nationalarchives/da-tre-dev-documentation) after being [parsed](https://github.com/nationalarchives/tna-judgments-parser) to extract essential descriptive metadata - the `court` and `title`.
A message is sent to an SNS topic in the TRE account which is sent to the importer SQS queue in our account.

The [`Preingest Court Document Importer` Lambda](/scala/lambdas/preingest-courtdoc-importer/) is responsible for unzipping
and untarring the package from TRE and for copying the judgment file and metadata file to our raw cache bucket. 
The lambda then sends a message to the aggregator queue

The [`Preingest Court Document Aggregator` Lambda](/scala/lambdas/preingest-tdr-aggregator/) is responsible for batching up messages for ingest. 
These are batched either based on the number of messages received in a 10-minute window or when the number of messages reaches 10000.
The batches are written to DynamoDB and the preingest step function is triggered.

The [`Preingest Court Document Package Builder` Lambda](/scala/lambdas/preingest-courtdoc-package-builder/) is responsible for creating the DR2 ingest metadata which will be passed to the ingest process.
Once this has been created and uploaded to S3, an ingest step function is triggered.

