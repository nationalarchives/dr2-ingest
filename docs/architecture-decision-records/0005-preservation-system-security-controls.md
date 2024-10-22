# 5. Preservation System Security Controls

**Date:** 2023-07-03

## Context

The managed service instance of the Preservation System will be hosted by the supplier and accessible to all on the public internet. By default, the system uses username/password for authentication of both the UI and API. There is no requirement within the system to rotate passwords. As this system will be used to store both OFFICIAL (open records) and OFFICIAL-SENSITIVE (closed records) data, we’d benefit from defining additional security controls that meet our business needs.

## Decision

The Digital Preservation team undertook a STRIDE (Spoofing, Tampering, Repudiation, Information disclosure, Denial of service, and Elevation of privilege) review of the managed service solution as part of our Alpha stage; the outcome of which defined the following actions as requirements for security.

### Use SSO for all user authentication and authorisation

Human users must access the system through the organisation's Single Sign-On function. This will tie human user identities to their IT accounts, allowing us to benefit from the pre-exiting Joiners/Movers/Leavers policies used by IT. Authorisation can also be handled through this function by placing users into Active Directory groups that correspond with roles in the Preservation System. Again, we benefit from the pre-existing group admin policies defined by IT.

When the Preservation System is configured to use an SSO provider, the UI is locked down to prevent username/password authentication. Human users will not need to remember an additional password to use the system and users finding the login page through the internet will be redirected to SSO to authenticate before accessing.

### Automated rotation of API credentials

SSO prevents human user access using username/password authentication, but the API will still require this authentication method. The API is authenticated in 2 stages: firstly a 15 minute access token is generated using the username/password, then the access token is used as a header on the GET/PUT/POST/DELETE request made by the user. Although the access token is short-lived, leaked credentials can be used to generate a new access token and a user is not limited to the number of active tokens at one time.

To reduce the period of validity if credentials are unknowingly exposed, an automated process will be used to rotate API user credentials every few hours. The runbook for this service should also contain steps to take if credentials are knowingly exposed, in order to manually trigger password rotation.

### API users - principal of least privilege

As we do in AWS, our services should only have the access they require to carry out their task. Extending this principle into the Preservation System, our API user actions should be limited. This will not be perfect; as all of our API users will likely need to see all closed metadata, the permissions granted will be rather wide, but it can protect file content from being read by a compromised API user.

### Restrict access to the application to only managed devices and automated processes

As the Preservation System will be available on the public internet, there is potential for users to access the application from anywhere globally and on any device. When compared to the outgoing legacy system, this introduces additional risk as leaked credentials could be easily used, or a user’s device may be compromised, enabling credentials or session data to be replicated on another machine. Whilst the supplier is committed to creating a secure system, having a publicly available system increases the risk posed our to data.

We will work with the supplier to restrict access to a range of known IP addresses covering user access from: on-premise, staff managed devices through the IT VPN solution, and our automated processes running in our own AWS accounts. Traffic will continue to flow through the public internet between us and the application, but will be encrypted using HTTPS.
