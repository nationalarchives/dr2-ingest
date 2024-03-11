# DR2 Get Latest Preservica Version

This is a Lambda which will get the version of Preservica that we are currently using and then call the Preservica client method
with a Preservica demo API endpoint ("retention-policies" for now as it doesn't require an Entity ref, and it's the least
impactful API call) in order to extract the version from the namespace. If there is a difference between the version we
are using and the version that Preservica are using, then publish a message (that contains the new version) to SNS.

## Input
The lambda is triggered from an AWS EventBridge schedule so the input is [a standard scheduled event](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-run-lambda-schedule.html#eb-schedule-create-rule).

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

## Environment Variables

| Name                                  | Description                                                               |
|---------------------------------------|---------------------------------------------------------------------------|
| PRESERVICA_DEMO_API_URL               | The Preservica demo API url                                               |
| PRESERVICA_SECRET_NAME                | The secret used to call the Preservica API                                |
| PRESERVICA_VERSION_EVENT_TOPIC_ARN    | The Preservica SNS topic to post Preservica version update to             |
| CURRENT_PRESERVICA_VERSION_TABLE_NAME | The table that stores the current version of Preservica that we are using |
