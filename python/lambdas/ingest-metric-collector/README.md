# DR2 Ingest Metric Collector
This lambda, when invoked, collects multiple metrics related to ingests at that point in time and sends the consolidated metrics to CloudWatch 


## Input
This lambda is triggered periodically (e.g. once every minute)

## Output
The lambda does not return anything, it sends metrics to CloudWatch

## Steps
1. Gets current lambda function name from context and gets the prefix from it, this decides the environment (e.g. intg, prd etc.) to collect the metrics from
2. Gathers the metrics from step functions 
   i.  Total number of executions running at that point in time
   ii. Number of running executions for each source system
3. Gathers the metrics from flow control
   i.  Total number of ingests that are queued at that point in time for each source system 
   ii. Time for the oldest queued ingest for each source system
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