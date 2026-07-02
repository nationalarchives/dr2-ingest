# 27. Lambda Aliases for Continuous Deployment of Step Function Lambdas

**Date:** 2026-07-02

## Context
The preingest, ingest and run workflow step functions each invoke a number of Lambda functions 
as part of their states. These Lambda invocations pointed directly at the Lambda function, 
which always resolves to the `$LATEST` version of the code.

We wanted to move towards continuous deployment, where changes are deployed automatically as soon
as they are merged, rather than being batched up and released at a fixed time. This exposed a
problem with pointing at `$LATEST`: an ingest can take a long time to run, potentially several
hours for large transfers, and it is common to have many ingests running concurrently at any given
time. If a deployment happened while executions were in progress, those in-flight executions would
immediately start invoking the newly deployed Lambda code the next time they reached a Lambda
task, rather than continuing to use the code they started with.

This caused real problems in practice. A change intended for new executions could unexpectedly
alter the behaviour of ingests that were already partway through, for example, by changing the
shape of the input or output a Lambda expected, or by altering business logic partway through a
workflow that assumed consistent behaviour from start to finish. This risked failed or
inconsistent ingests and made deployments unpredictable.

To avoid this, we had been restricting deployments to quiet periods, or waiting until no ingests
were running before deploying while preventing new ingests starting. This was manual, slowed down delivery, 
and did not scale as the number and frequency of ingests increased, working directly against the goal 
of continuous deployment.

## Decision
We will invoke every Lambda within the ingest Step Functions via a version alias
(for example, `prod`), rather than invoking the Lambda function directly or a specific
numbered version. On every deployment, new Lambda versions are published and the
alias is moved to the latest version before the Step Function resources are updated.

## Advantages
* **Continuous deployment without downtime**: new Lambda code can be deployed at any time,
  including while Step Function executions are in progress, without needing to wait for existing
  executions to finish or risk them failing mid-run.
* **In-flight execution stability**: an already-started Step Function execution resolves the
  alias ARN to a specific Lambda version at invocation time. Existing runs continue to operate
  consistently against the version that was current when they were invoked, unaffected by a
  Lambda deployment happening in parallel.
* **Simplified release process**: there is no need for manual coordination, feature flags, or
  scheduled deployment windows to avoid disrupting live Step Function executions, which
  simplifies the CI/CD pipeline.
* **Straightforward rollback**: because the alias is just a pointer to a version, reverting to a
  previous Lambda version is simply a case of repointing the alias, without needing to redeploy
  the Step Function definition.

## Disadvantages
* **Additional Terraform complexity**: the `create_lambda_alias` module and its explicit
  `depends_on` relationships add an extra layer of indirection that must be understood and
  maintained by anyone working on the infrastructure.
* **Version and alias drift risk**: if the alias update fails or is skipped, for example due to a
  partial apply, the Step Function could continue silently invoking an old Lambda version rather
  than failing loudly, delaying detection of a deployment issue.
* **Increased number of Lambda versions**: every deployment publishes a new Lambda version, which
  increases the number of stored versions over time and may require periodic clean-up to avoid
  hitting service limits.
* **`local-exec` provisioner dependency**: the alias update relies on a `local-exec` provisioner
  invoking the AWS CLI directly, rather than a native Terraform resource. This is less
  declarative, harder to plan, and depends on the AWS CLI being available and correctly
  configured in the environment running Terraform.
* **Debugging indirection**: when investigating a running or completed Step Function execution,
  developers must additionally check which Lambda version the alias pointed to at the time of
  execution, rather than reading the version directly off the Step Function definition.
