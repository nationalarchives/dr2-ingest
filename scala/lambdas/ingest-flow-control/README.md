# DR2 Ingest - Flow Control

A lambda which controls the flow of ingests into the Preservation System (PS) such that we neither overwhelm the system, nor keep too many tasks waiting.
This lambda is invoked twice, once, right before the ingesting into the PS (the "dr2-ingest-run-workflow" ingest) to ensure
a "flow-controlled" ingest and again, right after the ingesting has completed.

## Lambda input
The input to this lambda is provided by the Step Function.

```json
{
    "executionName": "TDR_batch-id",
    "taskToken": "some-task-token-which-identifies-a-task",
    "totalAssetCount": 10,
    "totalFileBytes": 1024
}
```

## Flow Control Config
The configuration (stored in AWS SSM) which governs how the flow of various tasks is controlled. A typical configuration looks as shown below:

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
  - `probability` comes into picture when there are free channels to schedule an ingest process. 
     When such situation arises, the scheduling is done based on the probability allocated to each of the system (e.g. a probability of 65 means, there is 65% chance given to that system to use next free channel).


## Lambda steps

It carries out the operations as follows:

1. The Lambda makes a call to SSM to get the flow control config. It adds it to a class object which validates the config for:
   1. Reserved channels should not be fewer than zero
   2. Probability must be between 0 and 100
   3. The probability of all systems together should equate to 100%
   4. Total of reserved channels should not exceed maximum concurrency
   5. There should not be duplicate system names
   6. Configuration must include a 'DEFAULT' system
2. The lambda makes use of a DynamoDB table to maintain a queue of tasks.
   1. If the lambda is passed an input and the `taskToken` is not `null` nor an empty string
      1. It extracts the source system name from the `executionName` in the Input (it splits the text by a "_" and takes the first part)
      2. It looks up this source system name and in the `systemName` of the source systems in the flow control config,
         1. if it is there, it just returns the sources system name
         2. if it's not there, then it returns the source system name of "default"
      3. It generates the current time and concatenates it with the `executionName` (from the input) separated by a "_"
      4. It then writes 
         1. `systemName` (used as primary key)
         2. an entry of the format `currentTime_executionName` (used as sort key)
         3. `taskToken`
         4. `executionName`
         5. `totalAssetCount` 
         6. `totalFileBytes` all as one item into the DynamoDB Queue table.
   2. If the input is empty (i.e. lambda is invoked after the "flow controlled ingest", no entry is written to the database; such an invocation only progresses any existing running execution. 
3. It retrieves all the running step function executions
4. It then checks if the `enabled` attribute (on the Input) is set to:
   1. `false`, if so the lambda exits immediately with the value `TaskOutput("FLOW_CONTROL_DISABLED", executionName)`.
   2. `true`, but the number of running executions is greater than or equal to `maxConcurrency`, then the lambda exits immediately with the value `TaskOutput(executionName, executionName)`.
   3. `true`, but the number of running executions is less than `maxConcurrency`, then each invocation of the lambda sends task success to, at most, one task.
      1. It totals all the reserved channels specified in the config, if there are reserved channels:
         1. It groups the running step function executions by source system name and gets the number of executions per source system.
         2. It iterates over all the systems listed in the config
            1. It gets the number of executions per source system and compares it to the number of reserved channels allowed for the source system
            2. If the number is greater than or equal to the reserved channels number,  it carries on iterating over the remaining systems to try and call "sendTaskSuccess" on any running execution
            3. If the number is less than the reserved channels number:
               1. It queries the Queue table using the source system to find all items (tasks) for that source system. Since the iteration is done based on system names rather than executions, it is possible that an execution started by one system may progress a waiting execution from another system.
               2. If there are tasks for the system, it calls "sendTaskSuccess" for the first task (item) in the list (the list is ordered by `currentTime_executionName`)
                  1. If there is a timeout error (i.e. "sendTaskSuccess" fails to resume the task), it will delete the task from the Queue table and call "sendTaskSuccess" with the next task in the list
                     * It will continue to do this (call sendTaskSuccess for the next queued task) as long as there is a timeout error and once it's gone through all tasks, it will run move onto the next source system
                  2. If the call to sendTaskSuccess is successful, then it deletes the task from the Queue table and returns `TaskOutput(executionName, executionName)`
                  3. If any other type of error is returned, it will throw this error and terminate the lambda.
         3. If it doesn't manage to start a task on a reserved channel, it then calculates whether there are spare channels in the system. 
            1. It adds the number of running executions to the number of reserved channels, excluding those reserved channels which are already in use.
            2. If this is less than the maximum concurrency, then there are spare channels.  
            3. If there are spare channels, it will try to schedule a task based on probability (weight).
               * See "[Processing based on probability](#processing-based-on-probability)" section for more.
            4. If there aren't, then it returns `TaskOutput(executionName, executionName)`
      2. If there are 0 total reserved channels, it will it try to schedule a task based on probability (weight), specified in the config.
         * See "[Processing based on probability](#processing-based-on-probability)" section for more.
      3. Once it successfully schedules a task (either on reserved channel or through probability), the lambda invocation terminates.
      4. If neither the reserved channels, nor probability approach schedules a task (e.g. no waiting task), the lambda invocation terminates.

### Processing based on probability

For progressing a task based on probability, it iterates over all the systems in the configuration by systemName
   1. It calculates the probability range for the system
      * for e.g., based on the probabilities in the config above: `{TDR: 1-20, COURTDOC: 21-40, DEFAULT: 41-100}`
   2. It generates a random number between 1 and 100 (both inclusive) and if the random number falls within the probability range of the system, it calls "sendTaskSuccess" for that system and invocation of this lambda eventually terminates.
   3. If the selected system does not have a "running" execution, it excludes that system and regenerates the probability ranges for the remaining systems.
   4. It continues this process until it finds a system with a running execution and the random number falls within the probability range of that system.
   5. Once it finds such a system, it calls "sendTaskSuccess" for that system and invocation of this lambda eventually terminates.
   6. If it cannot find a system with a running execution, it terminates the lambda invocation.

## Error handling
At times, it is possible that more than one invocation reads the same item(s) from the dynamoDB table;
in such a case, the first invocation succeeds and deletes the item. Any subsequent invocations encounter an error condition;
these subsequent invocations simply delete the item and continue processing remaining systems from the configuration.
