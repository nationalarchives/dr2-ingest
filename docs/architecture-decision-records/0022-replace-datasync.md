# Architectural Decision Record (ADR): Switching from AWS DataSync to Direct Lambda-to-Preservica Transfers

## Context
In our current architecture, the opex creator lambda creates the opex files in our staging AWS S3 bucket. These files are later transferred to Preservica, which uses an S3 bucket in their AWS account.

To facilitate this transfer, we used **AWS DataSync**. This approach was initially chosen to simplify permissions, adhering to the principle of least privilege. Instead of allowing multiple Lambdas to directly access the Preservica bucket, we used a single IAM role for DataSync. However, this approach has revealed significant performance limitations:

- **Slow transfer speed**: DataSync’s transfer rate does not meet our requirements, especially during periods of high file volume.
- **Concurrency limits**: DataSync has a limit on the number of concurrent tasks, causing bottlenecks when multiple file transfers are queued.

Given these limitations, we evaluated alternative approaches to improve performance while maintaining secure and manageable permissions.

## Decision
We will replace the use of AWS DataSync with a mechanism where each Lambda function directly transfers files to Preservica’s S3 bucket. This will involve:

1. **Updating Lambda Roles**:
   Each Lambda will continue to use its own IAM role with scoped-down permissions based on its function.

2. **Role Assumption for Cross-Account Access**:
   To access Preservica’s bucket, each Lambda will assume a designated IAM role (shared across all Lambdas) that has the necessary `s3:PutObject` permissions on Preservica’s bucket. This role exists solely for the purpose of cross-account transfers.

This approach simplifies the transfer process and eliminates the need for intermediate storage or batch processing with DataSync.

## Rationale
The new solution addresses both performance and security requirements:

1. **Performance Improvement**:
   - Direct Lambda-to-Preservica transfers avoid the overhead and concurrency limits of DataSync.
   - Files are transferred immediately upon creation, reducing latency.

2. **Adherence to the Principle of Least Privilege**:
   - Each Lambda retains its scoped IAM role for regular operations, ensuring isolation of permissions.
   - The shared cross-account role is only used temporarily during the transfer process, minimising exposure.

3. **Simplified Architecture**:
   - Removing DataSync reduces the complexity of our system.
   - The new approach eliminates the need for managing DataSync tasks and schedules.

## Consequences

### Positive Consequences
- **Faster transfers**: File movement is now limited only by Lambda’s invocation capabilities and S3 performance.
- **Reduced system complexity**: One fewer AWS service to manage.
- **Improved scalability**: No task concurrency limits as with DataSync.

### Negative Consequences
- **Slight increase in Lambda complexity**: Lambdas now require additional logic to assume the cross-account role and handle potential failures during the file transfer.
- **Shared role risk**: While the shared role is tightly scoped, any misconfiguration could inadvertently grant broader access.

## Alternatives Considered
1. **Optimising DataSync**:
   - Increasing task limits and parallelisation were considered but would still not match the performance of direct transfers.

2. **Creating Dedicated Lambdas for Transfers**:
   - Introducing separate Lambdas solely for transferring files would complicate the architecture and potentially create similar concurrency issues.

## Decision Outcome
This approach has been approved for implementation. We will monitor the performance and security of the new system, ensuring the shared role is tightly managed and that file transfers meet our performance expectations.

## Appendix

### Assumptions
- Preservica’s bucket will continue to allow access via the shared IAM role.
- Lambda execution time and resource limits will not pose a problem for the largest files transferred.
