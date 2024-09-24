# Custodial copy queue creator
This is a Lambda which generates events for the custodial copy FIFO queue.

## Lambda steps
For an input from the entity event generator:
* If the message is for an IO entity, the message is sent to the custodial copy queue with a message group id of the IO id.
* If the message is for a CO entity, the lambda looks up the parent id from Preservica and a message is sent to the custodial copy queue with a message group id of the parent ID.
* If the message is for an SO entity, the message is ignored.

In each instance, a message deduplication id is necessary, but we don't want messages being deduplicated as we could lose updates in the custodial copy process. 
To prevent this, we set the deduplication id to a random UUID.

## Input
The lambda is triggered from an SQS queue. The body of the message is the same as the output from the entity event generator.

```json
{
    "id": "io:746f426f-1a17-4777-80b3-9dff2df41204",
    "deleted": false
}
```

## Output
The lambda has no output. It sends a message to the custodial copy queue.

## Infrastructure
[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments)

## Environment Variables

| Name                   | Description                                |
|------------------------|--------------------------------------------|
| PRESERVICA_API_URL     | The Preservica API  url                    |
| PRESERVICA_SECRET_NAME | The secret used to call the Preservica API |
| OUTPUT_QUEUE           | The url of the Custodial Copy FIFO queue   |


