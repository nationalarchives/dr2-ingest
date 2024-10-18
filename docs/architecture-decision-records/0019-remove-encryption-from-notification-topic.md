# 19. Remove encryption from notifications SNS topic

**Date:** 2024-06-28

## Context

We originally implemented KMS (Key Management Service) encryption on the notifications SNS (Simple Notification Service) topic to ensure tighter control over who could read the messages and to encrypt any potentially sensitive data at rest. The encrypted topic was intended to provide an additional layer of security for sensitive information.

However, our SNS topic will be subscribed to by multiple accounts, and coordinating with different teams to grant them access to the KMS encrypted topic has proven to be complex and time-consuming. Additionally, none of the content in the messages is sensitive, which makes the encryption overhead unnecessary.

## Decision

We have decided to remove KMS encryption from the SNS topic completely.

### Rationale

- No Sensitive Content: The messages published to this SNS topic do not contain any sensitive information. Therefore, encryption is not required to protect the data.
- Operational Overhead: Maintaining KMS encryption requires significant coordination with other teams to ensure they have the necessary permissions to access the topic. This process is cumbersome and introduces delays.
- Cross-Account Access: Using the standard AWS SNS key does not support cross-account access, which further complicates our architecture and operational processes. Removing KMS encryption simplifies cross-account subscriptions and access management.

## Consequences

### Positive Consequences:

- Simplified Access Management: Without KMS encryption, other accounts can subscribe to and access the SNS topic without additional configuration or permissions, streamlining the process.
- Reduced Operational Overhead: Eliminating the need to coordinate KMS access with multiple teams reduces administrative efforts and potential delays.
- Cost Efficiency: By removing KMS encryption, we avoid the costs associated with KMS usage, contributing to overall cost savings.

### Negative Consequences:

- Loss of Encryption: Messages in the SNS topic will no longer be encrypted at rest, which might be a concern if the message content changes in the future to include sensitive information. Unlike other AWS services (S3, SQS, etc.), AWS does not offer a low-maintainance cross-account encyrption method for SNS.
- Reduced Control: Without KMS encryption, we have less granular control over who can read the messages, potentially increasing the risk of unauthorised access.
- Ongoing architectural policy: We must ensure that the messages published to this topic do not contain sensitive information.

### Alternatives Considered

- Continue with KMS Encryption: This was rejected due to the operational complexity and overhead involved in managing cross-account access.
- Use the Standard AWS SNS Key: This was not feasible because the standard key does not support cross-account access, which is a primary requirement for our architecture.

### Conclusion

After careful consideration of the current message content and the operational challenges posed by KMS encryption, we have decided to remove KMS encryption from our SNS topic. This decision will simplify access management and reduce overhead without compromising the security of sensitive information, given that our messages are not sensitive.
