# 8. Abstract Preservation System from clients

**Date:** 2023-10-04

## Context

The Digital Preservation Service is required to ingest, store, and actively preserve digital records held by The National Archives. The service may also be required to provide access to held records for other systems; e.g. transferring newly opened records to Discovery. The service itself is based on a managed service provided by a third party supplier.

As the system is a third party product it will be subject to regular review of suitability and value. As a customer, The National Archives can request changes to the product but ultimately ownership and management of the product and developments is the responsibility of the supplier. There may come a time where the product is no longer suitable for our needs.

Given the Digital Preservation Service will hold our records and be dependency of other services, a decision to migrate to another product could impact many other services.

## Decision

We will abstract the functions and data models of the Preservation System within the Digital Preservation Service. Our business logic will be written objectively, calling generic methods that could be implemented for any backend system. For example, instead of calling `CreateStructuralObject`, a term used by the product, our functions will use `CreateFolder`. This will decouple our business logic from implementation, enabling The National Archives to change backend systems in future.

Clients of the Digital Preservation Service will interact with in-house developed functions which will translate requests into business logic and onto product specific actions. Clients will not interact directly with the product at any point, nor will they be presented data in a schema defined by product. If The National Archives choose to use a different backend system, clients will not be affected this change.

## Consequences

The Digital Preservation Service will be required to develop schemas for data transfer between services. “Wrapper” APIs will be developed around the Preservation System to enable requests from other services to be vendor agnostic.

We will create a Client library to be used within our business logic. There will be certain functions that require closer integration, like a function to build an OPEX package, these will be designed to take a generic input and output the product specific format.
