# DR2 Pause Preservica Activity

## Input

The lambda can be triggered either directly from a call to invoke-lambda in [GitHub Actions](https://github.com/nationalarchives/dr2-runbooks/actions/workflows/pause_and_resume_preservica_activity.yml) or can be triggered by an
Eventbridge scheduled event.

The invoke-lambda input will be either

```json
{
  "pause": true
}
```

or

```json
{
  "pause": false
}
```

depending on whether we want to pause and resume the ingest.

With the scheduled event input, we're only looking at the `source` field

```json
{
  "source": "aws.events"
}
```

## Output

The lambda doesn't return anything

## Steps

If `pause` is True

* Disable the rotation for all secrets
* Disable the entity event schedule
* Send a message to Slack to say that Preservica activity is being paused

If `pause` is False

* Enable rotation for all secrets
* Enable the entity event schedule
* Send a message to Slack to say that Preservica activity has been resumed

If `source` is `aws.events`

If any of the secrets still have their rotation disabled or if the entity event schedule is still disabled,
then send a message to say Preservica activity is still paused, otherwise do nothing

[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments)

## Environment Variables

| Name                    | Description                                                                                                                          |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| SECRETS_MANAGER_DETAILS | A json array of secret details in the form `{"id": "secret-arn-1", "lambda_arn": "lambda-arn-1", "schedule_expression": "4(hours)"}` |
| ENVIRONMENT             | The environment                                                                                                                      |
