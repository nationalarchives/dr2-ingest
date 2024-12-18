# DR2 Ingest - Flow Control

A lambda which controls the flow of ingest into preservica such that we neither overwhelm the system, nor keep too many tasks waiting.

## Lambda input
The input to this lambda is provided by the Step Function.

```json
{
	"executionId": "TDR-execution-id",
	"taskToken": "some-task-token-which-identifies-a-task"
}
```

## Flow Control Config
The configuration which governs how the flow of various tasks is controlled.

```json
{
  "maxConcurrency": 7,
  "sourceSystems": {
    "TDR": {
      "dedicated": 2,
      "probability": 25
    },
    "FCL": {
      "dedicated": 2,
      "probability": 60
    },
    "DEFAULT": {
      "dedicated": 1,
      "probability": 15
    }
  }
}
```


## Lambda steps

The Lambda validates the flow control configuration for:
1. Dedicated channels should not be fewer than zero
1. Probability must be between 0 and 100
1. The probability of all systems together should equate to 100%
1. Total of dedicated channels should not exceed maximum concurrency
1. There should not be duplicate system name
1. Configuration must include a 'default' system


The lambda operates based on the flow control configuration. Each invocation of the lambda sends task success to at most one task. It carries out the operations as follows:

- It makes use of a dynamoDB table to maintain queue of tasks. 
- On invocation, if there is a taskToken passed in, it adds the details alongwith the current timestamp into a dynamoDB table.
- It then reads the config, iterates over all the systems one at a time.
- when it finds an entry for a system in the dynamoDB table, and a free dedicated channel for that system, it calls `sendTaskSuccess` for that task and returns.
- If it goes through all systems, and it is unable to progress any task on dedicated channel, it goes over the same config for probability.
- It generates a random number between 1 and 100 (both inclusive) and tries to schedule a task based on the probability ranges.
- Once it successfully schedules a task, it returns.
- If neither the dedicated channels, nor probability approach succeeds in scheduling a task (e.g. no waiting task), it returns.