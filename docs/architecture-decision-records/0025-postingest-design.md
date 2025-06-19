# 25. Postingest Design

**Date:** 2025-03-26

## Context

We want to track the status of each asset after ingesting into the Preservation System and notify other services on progress as the assets move through Custodial Copy, tape storage, and offsiting of the tapes. 
Doing this will enable us to confirm to ourselves that each asset transferred is held securely and enables other services to delete their own transitory copies of assets.

Custodial Copy reacts to events that happen inside the Preservation System, but this means that our ingest process does not orchestrate Custodial Copy. 
As part of the work to build Custodial Copy we implemented a process to confirm assets have reached Custodial Copy, we do this by
- Custodial Copy notifies us when it processes an entity.
- We write this status to the Asset (IO) and File (CO) items in the dr2-ingest-files table.
- We react to changes in the dr2-ingest-files table and **infer** an asset is complete when
  - the Asset item has field ingested_PS,
  - the Asset and File items all have field ingested_CC,
  - and the Asset is not part of another, incomplete batch within dr2-ingest-files.
- Once an asset is complete, we delete it from dr2-ingest-files.

![Current Custodial Copy confirmation workflow](/docs/images/adr/0025/current-cc-workflow.png)

Whilst this process often works, we have observed issues. For example, a failure in this process can cause an asset to get stuck in the dr2-ingest-files table, preventing the asset from ever sending a success notification; 
this problem is most likely to occur when an ingest to the Preservation System fails as we do not have a process to clean up the dr2-ingest-files table after this event. 
We also send multiple messages for each stage of our pipeline, causing downstream services more processing and preventing us from gathering meaningful metrics. 
And, due to encryption, we are unable to see within the dr2-ingest-files table to identify these problems in operation. 
Finally, with just a single stage in our pipeline (Custodial Copy), the dr2-ingest-files-change-handler Lambda is already overcomplicated and we’ll need to add extra steps for tape and offsiting in the future.

## Decision

We will build a new "Postingest" part of our application. This will be responsible for handling checks after we have reconciled an asset within the Preservation System and handle assets seperately, whereas Ingest handles assets within batches.

As it is likely that stages in our postingest pipeline will take longer than 2 weeks, we are unable to orchestrate this through SQS alone. Instead, we will use SQS to deliver events to workers, to benefit from the built in retry and concurrency handling, but store state in DynamoDB. We will react to changes in our DynamoDB table to send messages to workers via SQS and resend messages that have expired out of the SQS queues.

We will decouple the reporting from our exiting Custodial Copy worker, creating a new "Custodial Copy Checker" worker with the sole purpose of finding an item with a given ID in our OFCL repository and reporting that it exists. We have already made the [necessary changes to Custodial Copy](https://github.com/nationalarchives/dr2-custodial-copy/pull/256) to support this. As additional stages are added to our pipeline, we will add queues and workers for these.

## Consequences
- Assets should no longer get stuck after a failure.
- The change handler lambda on the postingest table will be much simpler and easier to maintain.
- The built-in retries in SQS will make the system more resilient. 


### Negative consequences
- There are more infrastructure resources needed. There are extra SQS queues, another Dynamo table and extra ECR repositories for the docker images. These resources need to be maintained and monitored for security issues which takes time. Also, our costs will increase slightly.
- The docker images stored in the repositories will need to be scanned for vulnerabilities and those vulnerabilities addressed if found. 
- There will be extra load on the DRI servers used to run the custodial copy confirmer and later the Scout AM confirmer. 
- The CC confirmer will need to read from the OCFL repository which may cause extra requests to the tape drives.
