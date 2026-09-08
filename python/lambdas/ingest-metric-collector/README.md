# DR2 Ingest Metric Collector
This lambda, when invoked, collects multiple metrics related to ingests at that point in time and sends the consolidated metrics to CloudWatch 


## Input
This lambda is triggered periodically (e.g. once every minute)

## Output
The lambda does not return anything, it sends metrics to CloudWatch

## Steps
1. Gets current lambda function name from the `context` argument passed to it,
   1. Splits it by "-" and gets the first part, the prefix (which is the environment e.g. intg, prd etc.), from it; 
   2. It then appends "-dr2-ingest" it (e.g. "intg-dr2-ingest") and calls it the `resources_prefix`
2. Gathers the metrics from step functions 
   1. Gets all a list of all executions running at that point in time, per state machine, and saves (as a metric 
      named "ExecutionsRunning") the:
      1. The total
      2. The state machine arn
      3. The state machine name 
   2. For state machines that start with the `resources_prefix`, gets all a list of all executions running at that point in time, per state machine,
      1. Gets the source system names from the executions
      2. Generates a count per source system
      3. Adds non-source systems count to a "DEFAULT" group
      4. And saves (as a metric named "ExecutionsRunning") the:
         1. The counts per source system
         2. The state machine arn
         3. The state machine name
      5. For each execution
         1. Gets the execution history events
         2. Finds the event that has the type of `TaskStateExited` (i.e. task has finished) and if the `name` in the
            `stateExitedEventDetails` object matches the Mapper lambda name as this is the first place where the 
            total asset count and total bytes are calculated and output
            1. If it's not there, then throw an error
            2. If it's there
               1. Gets the `output` from the `stateExitedEventDetails`
                  * If `output` doesn't exist, it throws an error
               2. From that output, extract the `totalAssetCount` and `totalFileBytes` values
               3. And saves the
                  1. `totalAssetCount` and source system (extracted from the execution name) to a metric
                     called `AssetCount`
                  2. `totalFileBytes` and source system (extracted from the execution name) to a metric
                     called `Bytes`
3. Gathers the metrics from flow control
   * For each source system
      1. Queries the DynamoDB Queue table for the items that have the source system that are queued at that point in time
      2. Saves the items per source system
      3. Gets the first item (the oldest) and gets the value for `queuedAt`; makes the value 0 if there are no items
      4. Saves this `queuedAt` time (as a metric named "ApproximateAgeOfOldestQueuedIngest")
      5. For each item
         1. gets the value for `queuedAssetCount` and saves is (as a metric named "QueuedAssetCount")
         2. gets the value for `queuedBytes` and saves is (as a metric named "QueuedBytes")
4. It then combines these two sets of metrics and sends them to CloudWatch 
5. If any call to collect metrics fails, it carries on collecting subsequent metrics and sends available metrics to CloudWatch
6. If all calls to collect various metrics fail, the lambda raises an exception 
7. If the lambda fails to send metrics to CloudWatch, it raises an exception

## Example metrics json
Following snippet shows example of the metrics json sent to CloudWatch
```json
[
    {
        "Dimensions": [
            {
                "Name": "SourceSystem",
                "Value": "TDR"
            }
        ],
        "MetricName": "IngestsQueued",
        "Unit": "Count",
        "Value": 1
    },
    {
        "Dimensions": [
            {
                "Name": "SourceSystem",
                "Value": "TDR"
            }
        ],
        "MetricName": "ApproximateAgeOfOldestQueuedIngest",
        "Unit": "seconds",
        "Value": 60.001152
    },
    {
        "Dimensions": [
            {
                "Name": "SourceSystem",
                "Value": "COURTDOC"
            }
        ],
        "MetricName": "IngestsQueued",
        "Unit": "Count",
        "Value": 0
    },
    {
        "Dimensions": [
            {
                "Name": "SourceSystem",
                "Value": "COURTDOC"
            }
        ],
        "MetricName": "ApproximateAgeOfOldestQueuedIngest",
        "Unit": "seconds",
        "Value": 0
    },
    {
        "Dimensions": [
            {
                "Name": "SourceSystem",
                "Value": "DEFAULT"
            }
        ],
        "MetricName": "IngestsQueued",
        "Unit": "Count",
        "Value": 0
    },
    {
        "Dimensions": [
            {
                "Name": "SourceSystem",
                "Value": "DEFAULT"
            }
        ],
        "MetricName": "ApproximateAgeOfOldestQueuedIngest",
        "Unit": "seconds",
        "Value": 0
    }
]
```

## Environment Variables

| Name                     | Description                          |
|--------------------------|--------------------------------------|
| SOURCE_SYSTEMS           | A stringified list of source systems |
| MAPPER_LAMBDA_STATE_NAME | The name the Mapper lambda uses      |
