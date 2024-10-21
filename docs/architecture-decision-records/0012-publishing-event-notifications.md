# 12. Publishing event notifications

**Date:** 2024-05-09

## Context

Other services within the organisation may want to act upon changes within the Preservation Service, for example, transfer systems deleting their copy of a record once it has been ingested to ours.

## Decision

We will create an SNS topic and use this to publish notification events for consumers.

## Consequences

We will create a SNS topic and publish our notifications exclusively to this topic. We will be responsible for subscribing consumers to our topic when required, this action will be considered part of DR2 BAU support.

As consumers will be subscribed directly to our topic, we will know who is consuming our events and include these services as stakeholders when making changes.

We will implement logging of events published to our SNS topic to enable auditing.

We will create schemas for our messages and document these for consumers.
