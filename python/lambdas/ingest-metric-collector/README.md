# DR2 Ingest Metric Collector
This lambda, when invoked, collects multiple metrics related to ingests at that point in time and sends the consolidated metrics to cloudwatch 


## Input
This lambda is triggered periodically (e.g. once every minute)

## Output
The lambda does not return anything, it sends some metric to cloudwatch

## Steps
1. Gets current lambda function name from context and gets the prefix from it, this decides the environment (e.g. intg, prd etc.) to collect the metrics from
2. Gathers the metrics from step functions 
   i.  Total number of executions running at that point in time
   ii. Number of running executions for each source system
3. Gathers the metrics from flow control
   i.  Total number of ingests that are queued at that point in time for each source system 
   ii. Time for the oldest queued ingest for each source system
4. It then combines these two sets of metrics and send it to cloudwatch 
5. If one of the metric collection fails, it carries on and sends the available metrics to cloudwatch
6. If all metrics collection fails, the lambda raises an exception 
7. If the lambda fails to send metrics to cloudwatch, it raises an exception

## Example metrics json
Following snippet shows example of the metrics json sent to cloudwatch
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