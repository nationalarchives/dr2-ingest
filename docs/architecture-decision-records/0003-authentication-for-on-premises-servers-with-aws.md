# 3. Authentication of on-premises servers with AWS

**Date:** 2023-07-05

## Context

As an organisation, we have an on-premises server that needs to integrate with our Amazon Web Services (AWS) infrastructure. The server hosts critical data which will be used for our custodial copy workflow. We need to establish a robust and secure authentication mechanism to allow the server to access the resources it needs in AWS and to minimise the risk of data or credentials being leaked or the backup data being lost.

## Decision

We have decided to authenticate the on-premises server with AWS using IAM (Identity and Access Management) users.

### Alternatives Considered

#### IAM Roles Anywhere

This uses a certificate provided by a certificate authority (either within AWS or within TNA) to authenticate with AWS.

This adds a layer of complexity to the process because we have to obtain a certificate from the certificate authority and install it periodically on the server. The AWS credentials obtained using this method are temporary but the certificate will be stored on the server so if the server is compromised, this method offers no additional security over using IAM users.

#### IAM Identity Centre

This uses a connection to Microsoft Active Directory to authenticate with AWS. Like when using certificates, the credentials are temporary but has the same problems in that it is more complex to implement and still needs static credentials to be stored on the server.

#### OIDC

This uses JWT tokens from an authentication server which are then swapped for temporary IAM credentials. This suffers from the same problems with increased complexity for almost no increase in security.

## Consequences

### Advantages

- Setup is simple and doesn’t need input from other departments
- We have an existing tool, Cloud Custodian, which will warn us about credentials which are too old and disable them if they are not regenerated in time.
- The policy attached to the user will only have permissions to assume another role which will have all of the permissions the custodial copy service needs to run. This role can be restricted to the IP address of the server which should minimise the risk if the credentials are compromised.

### Disadvantages

- IAM users use long-lived access keys. They are static credentials that remain valid until explicitly revoked. If these keys are compromised or leaked, an attacker has an extended period to exploit them for unauthorised access or malicious activities. This risk can be managed by rotating the keys frequently and having an automated process which will disable keys when they are too old.
- If an overly permissive policy is attached to the user, it can allow attackers access to a wider range of services. This can be mitigated by ensuring that the principle of least privilege is applied to the policies and making sure any changes are tracked and reviewed in source control.
- IAM users can have console access. This could lead to phishing attempts to get the username and password which would allow access to the console. For this reason, we should switch off console access.

### Conclusion

Using AWS IAM users to authenticate the on-premises server provides a secure. The ability to assume a role with restrictions on source IP outweigh the risks with having long lived credentials stored on a server. With these mitigation strategies in place, we can proceed with using IAM users for authentication with AWS on our on-premises server.
