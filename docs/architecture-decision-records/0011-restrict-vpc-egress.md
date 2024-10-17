# 11. Restricting egress from our VPC

**Date:** 2024-03-06

## Context

We need to connect to the Preservation System from our AWS account. It is IP locked, and changing IP addresses with them takes time and can’t be done automatically. We want to restrict our outbound security groups within AWS to only allow communication only with the system in order to prevent malicious code phoning home.

### Options Considered

| Option                                            | Pros                                                                         | Cons                                                                                                                                                                |
| ------------------------------------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Proxy via API Gateway with Lambda and NAT Gateway | Provides a static IP address for communication with the Preservation System. | Unsuitable for transferring very large files due to Lambda timeout limitations.                                                                                     |
| Fargate service with Nginx and ALB                | Avoids Lambda timeout issues for large file transfers.                       | Increased infrastructure maintenance compared to option 1.                                                                                                          |
| Request static IP Addresses from supplier         | Least maintenance and quick to set up.                                       | Dependency on the supplier's IP addresses; potential issues if they change.                                                                                         |
| GuardDuty with Lambda                             | May provide security insights and threat detection.                          | Relies on GuardDuty reporting effectively on Lambda code; not a direct control for restricting outbound access. Requires building a process to respond to findings. |

## Decision

After careful consideration, we have chosen **Option 3: Use the IP addresses provided by the supplier and add them to the security group**.

### Rationale

Option 3 is selected for the following reasons:

- Least Maintenance: This option requires minimal ongoing maintenance compared to the other options. It provides a straightforward and efficient way to restrict outbound access.
- Quick Setup: Adding IP addresses to the security group is a quick and simple setup process, allowing for rapid implementation.
- Reduced Dependency: By relying on provided IP addresses, we reduce the risk associated with potential changes in their infrastructure. It offers a more stable and predictable approach.

Whilst other options might offer specific advantages, such as static IPs or better handling of large file transfers, the chosen option aligns with our goal of minimising maintenance efforts and quickly establishing a secure connection to the Preservation System.

## Consequences

- Dependency on supplier IP addresses: The solution depends on the accuracy and timely update of IP addresses in our configuration. Any changes from the supplier must be promptly reflected in our security group configuration.
- Limited Control Over IP Addresses: We relinquish some control over the IP addresses used for communication, which may be a concern in certain scenarios.
- Potential for Future Consideration: If issues arise or new requirements emerge, it may be worthwhile to revisit the decision and assess alternative options based on the evolving needs of the system. Additionally, the GuardDuty option requires building a process to respond to findings: introducing an additional aspect of operational consideration.
