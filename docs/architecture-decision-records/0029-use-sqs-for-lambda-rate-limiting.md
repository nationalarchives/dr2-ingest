# 29. Use SQS for Lambda Rate Limiting

**Date:** 2026-09-15

## Context

When we receive many messages from upstream systems in a short burst, we [aggregate these into
groups of a configurable size](./0020-aggregation-for-tdr-preingest.md) and begin the dr2-ingest
Step Function rapidly; this results in many executions of the Step Function being started almost
simultaneously. Although we have [Ingest Flow Control](./0024-ingest-flow-control.md) to manage the
number of workflows running in the Preservation System, our ingest process makes API calls to the
Preservation System outside of the flow controlled steps. When many executions are running
simultaneously, we have no rate limiting in place for these API calls.

We make API calls in the `dr2-ingest-find-existing-asset` Lambda before the OPEX creation process,
to check if the asset is already within the Preservation System and prevent re-ingesting if so, and
after ingesting in the `dr2-ingest-asset-reconciler` Lambda, to confirm the full asset was ingested
as expected. Due to the latency introduced by the Preservation System workflows, we do not expect to
need rate limiting for the reconciliation step as it is unlikely for multiple executions to reach
this simultaneously, but have experienced a Preservation System outage caused by multiple OPEX
creations running in parallel. As we use Map Runs to process multiple assets concurrently within a
single Step Function execution, the risk of overwhelming the Preservation System is amplified.
Therefore, we will focus on rate limiting the `dr2-ingest-find-existing-asset` Lambda Function, but
would like a solution that could be implemented for other Lambda functions too.

![](/docs/images/adr/0029/current-step-function-extract.png)

## Options considered

### Option A: Configure Reserved Concurrency on the Lambda Functions

We can set the reserved concurrency on the specific Lambda functions, as we do for the
`dr2-ingest-upsert-archive-folders` Lambda. This would limit the number of concurrent executions of
the Lambda, effectively providing rate limiting for the API calls it makes. However, this provides
no buffering. When many Step Functions are attempting to invoke the Lambda simultaneously, it will
return a `TooManyRequestsException` when the reserved concurrency limit is reached; our Step
Function must then manage retries without visibility of the other running executions.


### Option B: Use SQS to buffer and rate limit Lambda invocations

![](/docs/images/adr/0029/option-b.png)

We could implement an SQS Queue between our Step Function and the Lambda Function. Where the Step
Function currently invokes the Lambda Function synchronously, it will instead send a message to an
SQS Queue with the Lambda event and a Task Token. The Step Function will send one message per Map
Run which will contain an array of Asset IDs, the length of this array is managed by the
`ItemBatcher` within the Step Function definition. The Lambda Function will be modified to expect an
SQS event containing this message, process the assets, and then use the Task Token to report back to
the Step Function. For added protection, the Step Function State will be configured with a timeout
to ensure it does not wait indefinitely for the Lambda to complete.

This approach provides both rate limiting and buffering. The SQS Queue will absorb bursts of
incoming messages, allowing the Lambda Function to process them at a controlled rate. We will use
the `MaximumConcurrency` option on the Lambda's Event Source Mapping to restrict the number of
concurrent invocations. We will not implement a Dead Letter Queue for this new SQS Queue, as we will
be notified of failures/lost messages through our existing alerts for Step Function failures.

This option will introduce latency and complexity, with a new architectural pattern involving SQS
and Event Source Mappings for the Lambda Function. Although we will try to catch errors within the
Lambda Function and SendTaskFailure back to the Step Function, some errors, like Lambda Function
timeouts, will not report back to the Step Function, relying on the configured Step Function timeout
instead.


### Option C: Move OPEX Creation into our Flow Controlled Ingest

We could move the 3 states responsible for OPEX creation into dr2-ingest to the
dr2-ingest-run-workflow Step Function, moving these states to be within our Flow Controlled Ingest.
We cannot just move the "Enter Flow Control" state earlier as this state looks at the number of
running dr2-ingest-run-workflow executions to determine if new executions should be allowed to
proceed.

Moving these states into the Flow Controlled Ingest limits the number of Lambda invocations to the
maximum concurrency of flow control multiplied by the maximum concurrency of the Map Runs.

## Decision

We will implement Option B: Use SQS to buffer and rate limit invocations of the
`dr2-ingest-find-existing-asset` Lambda.

We decided against Option A as it provides no buffering and would require the Step Function to
manage retries. The aforementioned `dr2-ingest-upsert-archive-folders` Lambda Function, with a
reserved concurrency of 1, already suffers from concurrency limitations, causing ingests to fail
when multiple executions attempt to invoke it simultaneously.

Option C was not chosen because it would require significant changes to the existing Step Function
workflows and would result in many ingests moving directly into the Flow Controlled Ingest after
validation; the ListExecutions API call made when entering Flow Control is eventually consistent and
may not accurately reflect the current number of running executions. We suspect that the variable
latency introduced by OPEX creation is preventing too many dr2-ingest-run-workflow executions from
starting simultaneously - if we moved the OPEX creation states into the Flow Controlled Ingest, we
would lose this natural throttling effect and need to re-architect Flow Control to be more robust
against sudden spikes in execution concurrency.

This decision covers rate limiting the `dr2-ingest-find-existing-asset` Lambda only. The
Reconciler's Preservation System API calls are lower risk, as discussed above, so applying the same
SQS-based pattern there is left as future work, should rate limiting prove necessary.
