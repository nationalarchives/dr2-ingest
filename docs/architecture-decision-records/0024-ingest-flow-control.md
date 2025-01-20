# 24. Ingest Flow Control

**Date:** 2024-10-21

## Context

Our Generic Ingest process transforms our DR2 BagIt-like package into an OPEX package and ingests this to the Preservation System; once an ingest workflow has started within the Preservation System, we routinely poll for the status and wait for it to complete. The workflows within the Preservation System consist of multiple processes, orchestrated by the Workflow manager, and run on the JobQueue server; the JobQueue server has 16 threads available, meaning only 16 processes can run at any time (across all environments).

During periods of high load, we can start more ingest workflow instances than there are threads available; when this occurs, our ingest workflows block each other as they wait for resources. This increases the elapsed time for all batches to ingest, which in turn increases the number of state transitions in our Step Function and Lambda invocations required to check the workflow status, increasing cost.

## Decision

We will implement a flow control mechanism within our Generic Ingest Step Function. The mechanism will surround only the Preservation System writing and workflow logic, allowing other (read-only) API actions to continue unimpeded.

We will use Step Function's [Task Token](https://docs.aws.amazon.com/step-functions/latest/dg/connect-to-resource.html#connect-wait-token) feature to pause processing until capacity if available. We will write these to DynamoDB; this has been chosen as the queue technology as it is likely we will want to prioritise specific source systems and report on additional metrics about the contents of a batch (asset count, sum of file sizes, etc.). We did consider alternatives, such as SQS, but this would require infrastructure changes for each new source system and lacks the capability to make stored batch metrics accessible without consuming the message.

We will implement 2 mechanisms to consume from the queue: a dedicated "channel" based approach, and a probability-based approach. Both will be useable at the same time. The dedicated "channel" approach will reserve a minimum number of "channels" for a source system, whereas the probability-based approach will aim to use all available channels whilst prioritising source systems with a weighting configuration.

### Implementation

![Flow Control Diagram](/docs/images/adr/0024/flow-control-diagram.png)

- We will create a new Flow Control Lambda function to implement the business logic required to pause and continue ingests.
- We will create a new DynamoDB table to act as the queue for paused ingests; the table will have a Partition Key for the source system (extracted from the Step Function Execution name) and a Sort Key for the time an item was added to the queue. When querying DynamoDB, the response will be ordered using the Sort Key.
- We will change the Court Document Preingest route to use the new Generic Ingest batch naming convention defined for TDR Preingest: `<sourceSystem>_<groupId>_<retryCount>`. We will do this to enable our Flow Control Lambda to identify source systems using only the running Step Function execution names.
- We will re-order our Generic Ingest steps to move the [Upsert Archive Folders Lambda](/scala/lambdas/ingest-upsert-archive-folders) later in our process, just before we start the Preservation System's ingest workflow. This is required as the increased latency between the start of both processes could result in the ArchiveFolders being modified within the Preservation System by users or other ingests, resulting in ingests failing as they are unable to find the folders they created.
- We will create a new Step Function to orchestrate the tasks that require flow control, allowing our Flow Control Lambda to identify and manage running ingest workflows using only the Step Function `ListExecutions` and `SendTaskSuccess` API endpoints. The new Step Function will be executed synchronously within our existing Generic Ingest Step Function.

## Consequences

- Latency: During periods of high load or when the configuration reserves "channels" for specific source systems, the latency of some batches to ingest will be increased as the Generic Ingest process pauses whilst awaiting a Task Token callback.
- Updated ArchiveFolders could revert to older metadata or flip between old and new metadata as ingest batches progress. The metadata for these folders is either passed to the ingest workflow within the [metadataPackage JSON file](/docs/metadataPackage.md) or collected from Discovery by the [Mapper Lambda](/scala/lambdas/ingest-mapper). As we are no longer treating all ingest batches as equal, it is possible that updated metadata is reverted due to an older batch being resumed. This is a consequence we are accepting.
