# 6. Developer access to production

**Date:** 2023-07-05

## Context

The production and staging instances of the Preservation System will be IP restricted to the office IPs and the public IPs of the DR2 VPCs. The IP allow-list is maintained by the supplier and cannot be easily changed. We need to decide if developer laptops will be allowed to connect to these instances and if so, what the best way to do it is.

### Options to allow developers to connect

There were two options considered for how to allow developer laptops to connect;

- Use AWS Client VPN. This is expensive. The endpoint alone is $75 per month and then 5c per hour for every connection.
- Spin up a bastion host and use AWS Systems Manager to set up a SOCKS5 proxy, then change the browser settings to use this proxy.

## Decision

We will **not** allow developers to connect to the Preservation System. This decision is based on the following reasoning:

- Security Concerns: User devices are deemed insecure, suggesting that allowing them direct network access. By restricting access, we can mitigate potential vulnerabilities and protect the integrity and confidentiality of the Preservation Service.
- Lack of Need: Developers have no requirement or legitimate business need to connect to the production environment from a dev laptop. Allowing access would only introduce unnecessary complexity and potential risks without providing any tangible benefits. Access through an IT managed device is still available for emergencies.
- Compliance and Risk Management: By preventing developers from connecting to the Preservation System, we can align with compliance requirements and minimize the potential impact of security incidents or data breaches. Restricting access is a proactive measure to ensure the service remains secure and complies with relevant regulations.
- Simplicity and Maintenance: Disallowing developer access simplifies the architecture by removing the need for the bastion instance. This decision reduces the complexity of the system, making it easier to maintain.

## Consequences

While the decision to restrict developer devices from connecting to the Preservation System provides several benefits, it is essential to consider the following consequences:

- Operational Support: The decision may require additional effort from the team to handle issues with the production system. Access is still available to the production instance through a managed device.
- Future Considerations: As the security landscape evolves and user device security improves, it may be necessary to revisit this decision. Regular assessments should be conducted to determine if there is a justifiable need for user access or if there are alternative secure mechanisms available to grant limited access while maintaining overall system security.
