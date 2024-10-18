# 13. Technology choice for ingest workflow

**Date:** 2024-08-01

## Context

As per [DA-ADR-3 AWS as preferred cloud provider](https://national-archives.atlassian.net/wiki/x/BYBIB), we are building our application on AWS using PaaS components where possible. AWS offer a range of components that we could use to build our service.

## Decision

We will build our service using serverless components where possible. Our stack will be based on S3, Lambda, DynamoDB, and Step Functions.

We have chosen these technologies as the expected workload for the service is extremely bursty; we will only ingest new items shortly after a new transfer has been completed or during a migration project. We expect our system to be idle for large periods of time. Whilst we considered autoscaling IaaS infrastructure, building our service using IaaS would require more ongoing operations and maintenance, which we feel is not feasible.

## Consequences

By using PaaS components we can benefit from the scalability of the cloud. We can use this to our advantage to rapidly ingest large quantities of items when required. This may also require additional architecture to manage the scaling to ensure downstream systems are not flooded with requests.

As per AWS’s Shared Responsibility Model, using PaaS components reduces the operational overhead of the system. Whilst we are still responsible for secure design and maintaining our application code/dependencies, AWS will manage the operation of infrastructure and security of the physical building and underlying OS. Our decision aligns with the [Goverment Cloud First Policy](https://www.gov.uk/guidance/government-cloud-first-policy).

Many of the services we are using are billed on a pay-as-you-go model, at a premium to self-managed infrastructure. In normal circumstances we will benefit from this as we only pay for the resources our system uses. However, in migration situations this may cause additional cost as our system will be processing abnormally large loads during this time.
