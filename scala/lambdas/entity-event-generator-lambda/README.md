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

The lambda has no output
