# 7. Single generic ingest function

**Date:** 2023-07-20

## Context

The Preservation Service is required to preserve packages from multiple sources; TDR, TRE, DRI (migration), digitisation projects, etc. Each of these sources has their own schema for data and describing a package of digital objects.

## Decision

To reduce the effort required to build and maintain a bespoke process for each source we will build a single generic ingest function.

## Consequences

Adding new sources to the Preservation System will be quicker as the central ingest functionality will already exist and be common across all ingest sources.

DR2 will be responsible for defining a schema to describe a package for input to their ingest process. Once this schema is defined and implemented each source will require a bespoke “pre-ingest” step to transform the source package into our schema.

Data schemas within the preservation system should be consistent between objects. The generic ingest process should not contain bespoke logic for each source.
