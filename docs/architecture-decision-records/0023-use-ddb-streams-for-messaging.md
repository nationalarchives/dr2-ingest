# 23. Architectural Decision Record: Using DynamoDB Streams for messaging

## Context

We needed to send messages to an AWS SNS topic at various points in our workflow. The workflow is predominantly coordinated using AWS Step Functions. However, there is an additional, separate process known as [Custodial Copy](https://github.com/nationalarchives/dr2-custodial-copy/), which runs outside of AWS.

Both of these processes update fields in a DynamoDB table upon completing specific steps in the workflow. This shared update mechanism presented an opportunity to streamline how messages are sent to the SNS topic.

## Decision Drivers

1. **Maintainability**: Minimise the number of places to update logic if notification requirements change.
2. **Centralisation**: Prefer a single location for notifications logic to simplify testing and updates.
3. **Integration**: Seamlessly integrate with existing AWS components.
4. **Complexity**: Avoid adding unnecessary complexity to the system.

## Considered Options

### Option 1: Multiple Direct Calls to SNS
Each step in the workflow would send a message to the SNS topic directly at the point where that step occurs.

- **Advantages**:
  - Simpler to implement.
  - Notifications logic is embedded within the workflow steps.

- **Disadvantages**:
  - Notifications logic is duplicated across multiple locations.
  - Any updates to the logic would require changes in many places, increasing the risk of errors.
  - Higher risk of inconsistencies if some steps are overlooked during updates.

### Option 2: Use DynamoDB Streams with a Lambda Function
Enable DynamoDB Streams on the table, triggering a Lambda function for `INSERT` and `MODIFY` events. The Lambda function would filter the events and handle the SNS notifications logic in a centralised manner.

- **Advantages**:
  - Centralised location for notifications logic.
  - Easier to add or modify SNS notifications without touching the workflow or Custodial Copy processes.
  - Decouples SNS notifications from the workflow, making the system more flexible.

- **Disadvantages**:
  - Slightly more complex to set up and maintain the Lambda function and stream configuration.
  - Adds a dependency on DynamoDB Streams and Lambda.

## Decision

We chose **Option 2: Use DynamoDB Streams with a Lambda Function**.

### Rationale
This option provides a centralised and maintainable solution for sending SNS notifications. By decoupling the notifications logic from the workflows, we ensure that updates can be made in a single location without impacting multiple processes. The slight increase in complexity is outweighed by the long-term maintainability and flexibility of this approach.

## Consequences

- **Positive**:
  - Simplified maintenance of notifications logic.
  - Easier to adapt to new requirements or additional notifications.
  - Reduced risk of inconsistency in notifications behaviour.

- **Negative**:
  - Increased initial setup complexity.
  - Dependence on DynamoDB Streams and Lambda.


