# 16. Generic ingest idempotency

**Date:** 2024-05

## Context

We’ve designed our ingest system using PaaS/Serverless services on AWS. The services we are using are well suited to an event-driven architecture, hence we have designed our ingest process to be event based. The event distribution services on AWS and used by Digital Archiving guarantee at-least-once delivery, meaning our system must handle events with idempotence. Due to the services we’re using, it’s likely that a single event may be received by multiple consumers simultaneously rather than repeated events over a large period of time. We had hoped that the Preservation System would handle idempotency of ingests for us, but we’ve experienced specific bugs/race conditions related to simultaneous receives that require us to implement additional controls.

Within our initial implementation we used the TDR Consignment Reference as the Step Function execution name to prevent the same ingest package being processed more than once within 90 days. However, this solution is not feasible in the long-term as it couples the size of an ingest batch to the user-decided size of a TDR consignment.

## Decision

![Diagram of Generic Ingest with these changes implemented](/docs/images/adr/0016/ingest-diagram.png)

We will implement a lock feature to prevent the same item being included in concurrent ingest processes. We will do this using DynamoDB with a conditional write using `attribute_not_exists(pk)`. We will use the TDR UUID as the identifier for the record, assetId. We will lock during the preingest process and remove the lock once reconciliation has completed successfully. When the lock is in place, other preingest processes that attempt to ingest the same asset will fail to lock the asset and error - the messages will remain in SQS until ingest is unlocked.

We will implement an additional check before the Asset OPEX Creator to find an existing asset with the same `id` in the Preservation System. We will use the `SourceID` field for this, with the value being the TDR UUID. Where an asset already exists in the Preservation System we will exclude this item from our OPEX package but continue to reconcile the against the asset in the new ingest package.

We will publish a notification of ingest to our notifications SNS topic each time reconciliation succeeds.

## Consequences

This change should prevent duplicates being created in the Preservation System.

This change means that we will be temporarily maintaining state outside of the Preservation System and event-driven process for the first time. This means that a failure of our process will require clean-up before it can be retried. There are certain circumstances where an item may become stuck in a locked state and require clean up, for example if reconciliation after ingest fails.

Systems that interact with us should find it easier to integrate with our notifications being idempotent. These systems may still need to implement error handling based upon our notifications but the quantity of errors we produce will be less.

The identifier we use to de-duplicate is provided by an upstream system. Our process will only work if the upstream system does not mutate the objects attached to this ID, nor re-use the ID. We can only hope that the UUID is treated the same way in other systems.
