# Court Document Preingest

Court Documents are delivered to DR2 from the [Transformation Engine (TRE)](https://github.com/nationalarchives/da-tre-dev-documentation) after being [parsed](https://github.com/nationalarchives/tna-judgments-parser) to extract essential descriptive metadata - the `court` and `title`. As Court Documents are transferred as small individual files, we are able to process the BagIt packages from TRE in a Lambda function to extract the file and metadata.

The [`dr2-ingest-parsed-court-document-event-handler` Lambda](/scala/lambdas/ingest-parsed-court-document-event-handler/) is responsible for this extraction and transformation into a package for our ingest workflow.
