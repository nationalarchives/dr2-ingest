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
   ],
  "enabled": true
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
2. Probability must be between 0 and 100
3. The probability of all systems together should equate to 100%
4. Total of reserved channels should not exceed maximum concurrency
5. There should not be duplicate system names
6. Configuration must include a 'DEFAULT' system


## Lambda steps

The lambda operates based on the flow control configuration.

The flow control config has an `enabled` attribute. If this is set to false, then the lambda exits immediately. If it is set to true, then 
each invocation of the lambda sends task success to, at most, one task. This lambda is invoked from two places, once before the "flow controlled ingest" and once after the "flow controlled ingest". It carries out the operations as follows:

1. It makes use of a dynamoDB table to maintain a queue of tasks. To achieve this, as soon as it is invoked, it adds a new item to the dynamoDB table.  
   1. On invocation, it extracts the systemName from the current execution, a taskToken is passed as input, and currentTime is generated when the lambda is about to write an item.
   2. It adds the systemName (used as primary key), an entry of the format `currentTime_executionName` which is used as sort key, a taskToken and the execution name as an item into dynamoDB table.
   3. If the input is empty (i.e. lambda is invoked after the "flow controlled ingest", no entry is written to the database, such invocation only progresses any existing running execution. 
1.It then reads the configuration, iterates over all the systems mentioned in the config to find a matching task in dynamoDB table. Since the iteration is done based on system names rather than executions, it is possible that an exeuction started by one system may progress a waiting execution from another system.
   4. Once it reads an entry from the configuration, it finds a list of all tasks from the dynamoDB table for that system. If there is a channel available for the system, it calls "sendTaskSuccess" for that system and invocation of this lambda eventually terminates.
   5. If there are not enough reserved channels for the system, it carries on iterating over the remaining systems to try and call "sendTaskSuccess" on any running execution
   6. If there are no reserved channels available for any system, it tries to schedule a task based on probability.
2. For progressing a task based on probability, it iterates over all the systems in the configuration by systemName
   1. It finds the number of free channels available and calculates the probability range for the system
   2. It generates a random number between 1 and 100 (both inclusive) and if the random number falls within the probability range of the system, it calls "sendTaskSuccess" for that system and invocation of this lambda eventually terminates.
   3. If the selected system does not have a "running" execution, it excludes that system and regenerates the probability ranges for the remaining systems
   4. It continues this process until it finds a system with a running execution and the random number falls within the probability range of that system
   5. Once it finds such a system, it calls "sendTaskSuccess" for that system and invocation of this lambda eventually terminates.
   6. If it cannot find a system with a running execution, it terminates the lambda invocation.
3. Once it successfully schedules a task (either on reserved channel or through probability), the lambda invocation terminates.
4. If neither the reserved channels, nor probability approach schedules a task (e.g. no waiting task), the lambda invocation terminates.


## Error handling
At times, it is possible that more than one invocation reads the same item(s) from the dynamoDB table. In such case, the first invocation succeeds and deletes the item. Any subsequent invocation faces an error condition. In such case, these subsequent invocations simply delete the item and continue processing remaining systems from the configuration.