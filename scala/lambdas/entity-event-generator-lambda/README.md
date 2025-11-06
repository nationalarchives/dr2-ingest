# Entity Event Generator
This is a Lambda which will call the Preservica API to get the entities that have been updated
since a specified date and then publish a message for each to SNS and update DynamoDB with the date
of the last action (performed on an entity) as long as that date is before the date that the event
was triggered.

## Input
The lambda is triggered from an AWS EventBridge schedule so the input is [a standard scheduled event](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-run-lambda-schedule.html#eb-schedule-create-rule).

We are only using the time portion of the event.

```json
{
    "time": "2015-10-08T16:53:06Z"
}
```

## Process
* Get the start value and `LastPolled` date from DynamoDB.
* Call the `/updated-since` API endpoint with the date and start value from DynamoDB as the parameters.
* If all entities in the list are deleted entities or if there is more than one page of results:
  * Set the last action date to be the same as the `LastPolled` date.
* If some of the entities are not deleted: 
  * (Since these parameters are ordered by ascending date) get the last entity in the list, excluding any deleted entities.
  * Get the event actions for this entity and get the last action date from the response.
* If the last action date is after the triggered date from the input json, do nothing.
* If the last action date is before the triggered date from the input json:
  * Send a message to SNS with the form `{"id":"io:3963e77c-ed9a-4ff7-8960-aa2ca48973af","deleted":false}`
  * Increment the start count by the number of updated entities. This will prevent resending messages if all entities are deleted.
  * Store the start count and the last updated time in Dynamo DB and return the number of messages sent to SNS.
  * If the number is more than zero, repeat the process from the second step.

The lambda is only set to run for 60 seconds so will often time out if there have been a large number of entities updated.
If this happens, the lambda will re-run again after 5 minutes and pick up the most recent `LastPolled` and `start` from DynamoDB.

## Output
The lambda has no output
