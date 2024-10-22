# 17. Add childCount attribute to dr2-ingest-files DynamoDB table

**Date:** 2024-06-10

## Context

We have a DynamoDB table where data is queried using a global secondary index (GSI) to find child items of a parent item. Occasionally, the index is not fully populated by the time we read the data, resulting in fewer child items being returned than expected. To address this issue, we need to implement a check to ensure all expected items are retrieved.

### Options considered

| Description                                                                                                                                                                                                                                           | Pros                                                                           | Cons                                                                                                                                                                                                                       |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| When writing data to the table, include an attribute that contains an array of primary key IDs of the expected child items. This allows for direct querying of these IDs instead of using the GSI.                                                    | Eliminates dependency on GSI.                                                  | Potential for large arrays which could cause the parent item to exceed DynamoDBs max item size quota (400KB).                                                                                                              |
| Introduce a wait period between writing and querying to allow the GSI time to fully populate before querying.                                                                                                                                         | Simple to implement.                                                           | Indeterminate wait time leading to inefficiency; potential unnecessary delays. DynamoDB do not publish a maximum time to populate GSIs, they only say GSIs “updated asynchronously, using an eventually consistent model.” |
| Add a `childCount` attribute to the parent item at the time of writing. When querying for child items using the GSI, compare the number of items returned with the `childCount` value. If they do not match, error and retry the query after a delay. | No risk of exceeding DynamoDB's item size limit; ensures eventual consistency. | Potential delays due to retries; additional error logs could introduce noise.                                                                                                                                              |

## Decision

We will add a `childCount` attribute to parent items in DynamoDB and check this when querying for children.

## Consequences

In addition to the consequences mentioned in the Context section, this decision will require implementation across our ingest workflow.

### Implementation

1. Schema Update: Add a `childCount` attribute to each parent item during the data write operation. This will be within the dr2-ingest-mapper Lambda function.
2. Modify query logic in all Lambdas that query for children;
   - Query the child rows using the GSI.
   - Compare the number of returned rows with the `childCount` value.
   - If they match, process the data as usual.
   - If they do not match, throw an error and retry the query after a short delay; we will use Lambda/Step Function's retry mechanism.
3. Retry Mechanism: Implement an exponential backoff strategy for retries to balance load and responsiveness.
