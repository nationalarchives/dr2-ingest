# 27. Lambda Aliases for Continuous Deployment of Step Function Lambdas

**Date:** 2026-07-02

## Context
The preingest, ingest and run workflow step functions each invoke a number of Lambda functions.
These Lambda invocations pointed directly at the unqualified Lambda function ARN, which always
resolves to the `$LATEST` version of the function.

We wanted to move towards continuous deployment, where changes are deployed automatically as soon as
they are merged, rather than being batched up and released at a fixed time. This exposed a problem
with pointing at `$LATEST`: an ingest can take a long time to run, potentially several hours for
large transfers, and it is common to have many ingests running concurrently at any given time. If a
deployment happened while executions were in progress, those in-flight executions would immediately
start invoking the newly deployed Lambda code the next time they reached a Lambda task, rather than
continuing to use the code they started with.

In practice this means that a change intended for new executions could unexpectedly alter the
behaviour of ingests that were already partway through. For example, by changing the shape of the
input or output a Lambda expected, or by altering business logic partway through a workflow that
assumed consistent behaviour from start to finish. This risked failed or inconsistent ingests and
made deployments unpredictable.

To avoid this, we had been restricting deployments to quiet periods, or waiting until no ingests
were running before deploying while preventing new ingests starting. This was manual, slowed down
delivery, and did not scale as the number and frequency of ingests increased, working directly
against the goal of continuous deployment.

## Decision
We will invoke every Lambda within the ingest Step Functions via a version alias (for example,
`v0-0-100`), rather than invoking the Lambda function directly or a specific numbered version. On
every deployment, new Lambda versions are published and the alias is moved to the latest version
before the Step Function resources are updated.

We have decided to use Lambda Aliases in addition to Lambda Versions to enable selective remapping
to new code without restarting running ingests. When a Step Function Execution is started the
definition becomes immutable for that execution; if we used only Lambda Versions, we would not be
able to fix code issues then redrive the failed execution, requiring a manual clean-up of data
resources for the failed execution. By using Lambda Aliases, a developer may manually repoint the
Alias to working version of the code when they know there has not been a breaking change.

As previous Aliases must continue to exist until all Step Function executions referencing them have
completed, careful management of alias lifecycle is required. Therefore, we will manage Lambda
Aliases outside of Terraform. Aliases will be created through Terraform invoking the AWS CLI and
deleted alongside inactive versions by a new Lambda Function (`dr2-delete-lambda-version`); we
expect Versions and Aliases to be inactive when older than 30 days.

## Advantages
* **Continuous deployment without downtime**: new Lambda code can be deployed at any time, including
  while Step Function executions are in progress, without needing to wait for existing executions to
  finish or risk them failing mid-run.
* **In-flight execution stability**: an already-started Step Function execution resolves the alias
  ARN to a specific Lambda version at invocation time. Existing executions continue to operate
  consistently against the version that was current when they were started, unaffected by a Lambda
  deployment happening in parallel.
* **Simplified release process**: there is no need for manual coordination, feature flags, or
  scheduled deployment windows to avoid disrupting live Step Function executions, which simplifies
  the CI/CD pipeline.
* **Straightforward rollback**: because the alias is just a pointer to a version, reverting to a
  previous Lambda version is simply a case of repointing the alias, without needing to redeploy the
  Step Function definition.
* **Ability to apply in-flight fixes**: if a critical issue is discovered in a Lambda function, a
  new version with the fix can be deployed and the previous aliases updated, allowing ongoing Step
  Function executions to use the fixed version without waiting for all executions to complete or
  restarting failed executions.

## Disadvantages
* **Additional Terraform complexity**: the `create_lambda_alias` module and its explicit
  `depends_on` relationships add an extra layer of indirection that must be understood and
  maintained by anyone working on the infrastructure.
* **Version and alias drift risk**: if the alias update fails or is skipped, for example due to a
  partial apply, the Step Function could continue silently invoking an old Lambda version rather
  than failing loudly, delaying detection of a deployment issue.
* **Increased number of Lambda versions**: every deployment publishes a new Lambda version, which
  increases the number of stored versions over time and will require periodic clean-up to avoid
  hitting service limits.
* **`local-exec` provisioner dependency**: the alias update relies on a `local-exec` provisioner
  invoking the AWS CLI directly, rather than a native Terraform resource. This is less declarative,
  harder to plan, and depends on the AWS CLI being available and correctly configured in the
  environment running Terraform.
* **Debugging indirection**: when investigating a running or completed Step Function execution,
  developers must additionally check which Lambda version the alias pointed to at the time of
  execution, rather than reading the version directly off the Step Function definition.
