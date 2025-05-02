# DR2 Ingest Step Function Timings

This project is for a lambda that is triggered by an EventBridge notification, every N minutes.

1. The Lambda receives the JSON String event and retrieves the time from it
2. It subtracts N minutes from it (the last time it ran) as saves that to a variable 
3. It then gets the workflow sfn executions that have occurred from N minutes ago up until now
4. It gets the information of the ingest step function that have occurred from N minutes ago up until now
   1. If the ingest is a TDR one, it would have been triggered by a preingest step function, so it gets the 
      information of that
   2. It also finds the workflow step function that was triggered within the ingest step function
5. Once it has all the step function information, it then extracts the name, start date and end date into a Python dict
6. Finally, it coverts the dict into JSON, uploads the information to a JSON file (per ingest) in an S3 bucket
   * example of the JSON contents
   ```json
                {
                    "name": "ingestExecutionName",
                    "steps": {
                         "preingest": {
                            "endDate": 1735776000.0,
                            "name": "preingestExecutionName",
                            "startDate": 1735776000.0
                        },
                         "preservicaIngest": {
                             "endDate": 1735776000.0,
                             "name": "ingestExecutionName",
                             "startDate": 1735776000.0
                         },
                        "preservicaWorkflow": {
                            "endDate": 1735776000.0,
                            "name": "ingestExecutionName-nameOfWorkflowExecution",
                            "startDate": 1735776000.0
                        }
                    }
                 }
   ```


## Environment Variables

| Name               | Description                                                                               |
|--------------------|-------------------------------------------------------------------------------------------|
| INGEST_SFN_ARN     | The state machine arn of the ingest step function                                         |
| WORKFLOW_SFN_ARN   | The state machine arn of the workflow step function that the ingest step function calls   |
| PREINGEST_SFN_ARN  | The execution arn of the preingest step function (that triggers the ingest step function) |
| OUTPUT_BUCKET_NAME | The S3 bucket where the timings are output to                                             |


