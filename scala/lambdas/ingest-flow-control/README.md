# DR2 Ingest - Flow Control

A lambda which controls the flow of ingests into Preservica such that we neither overwhelm the system, nor keep too many tasks waiting.

## Lambda input
The input to this lambda is provided by the Step Function.

```json
{
	"executionId": "TDR-execution-id",
	"taskToken": "some-task-token-which-identifies-a-task"
}
```

## Flow Control Config
The configuration which governs how the flow of various tasks is controlled. A typical configuration looks as shown below:

```json
{
   "maxConcurrency": 8,
   "sourceSystems": [
      {
         "systemName": "TDR",
         "reservedChannels": 2,
         "probability": 20
      },
      {
         "systemName": "COURTDOC",
         "reservedChannels": 2,
         "probability": 20
      },
      {
         "systemName": "DEFAULT",
         "reservedChannels": 1,
         "probability": 60
      }
   ]
}
```

In the configuration shown above,
- `"maxConcurrency: 8"` indicates that there can be upto 8 ingest processes running at a time
- Each source system is configured with its `systemName` (e.g. "TDR", "FCL" etc.)
- Each source system has a configuration of `reservedChannels` and `probability`
- `reservedChannels` means there is a reserved channel out of the `maxConcurrency` for that specific system.
- `probability` comes into picture when there are free channels to schedule an ingest process. When such situation arises, the scheduling is done based on the probability allocated to each of the system. (e.g. a probability of 65 means, there is 65% chance given to that system to use next free channel)   


## Validations of the flow control configuration

The Lambda validates the flow control configuration for:
1. Reserved channels should not be fewer than zero
1. Probability must be between 0 and 100
1. The probability of all systems together should equate to 100%
1. Total of reserved channels should not exceed maximum concurrency
1. There should not be duplicate system names
1. Configuration must include a 'DEFAULT' system


## Lambda steps

The lambda operates based on the flow control configuration. Each invocation of the lambda sends task success to, at most, one task. It carries out the operations as follows:

1. It makes use of a dynamoDB table to maintain a queue of tasks. 
1. On invocation, if there is a taskToken passed in, it adds the taskToken as well as current timestamp into a dynamoDB table.
1. It then reads the config, iterates over all the systems mentioned in the config to find a matching task in dynamoDB table.
   1. If it finds an entry for a system in the dynamoDB table, and a free reserved channel for that system, it calls `sendTaskSuccess` for that task and the lambda invocation terminates. 
   1. If it cannot progress a task on reserved channel (e.g. no free channels), it attempts to progress a task based on probability in the configuration. If there is a free channel available, it generates a random number between 1 and 100 (both inclusive) and tries to schedule a task based on the configured probability ranges.
1. Once it successfully schedules a task (either on reserved channel or through probability), the lambda invocation terminates.
1. If neither the reserved channels, nor probability approach schedules a task (e.g. no waiting task), the lambda invocation terminates.

