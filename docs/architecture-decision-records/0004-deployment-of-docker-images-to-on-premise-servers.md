# 4. Deployment of Docker images to on-premise servers

**Date:** 2023-07-10

## Context

We have an on-premises server infrastructure where we need to deploy Docker containers. These containers will contain custom code which will pull files and metadata from AWS as part of our custodial copy workflow. The Docker images are stored in Amazon Elastic Container Registry (ECR). We need to make a decision on the tool or mechanism to automate the deployment of Docker images from ECR to the on-premises server.

## Decision

We have decided to use [Watchtower](https://containrrr.dev/watchtower/) as the tool for deploying Docker images from AWS ECR to our on-premises server infrastructure.

### Alternatives Considered

#### Using the Code Deploy agent

It is possible to install the Code Deploy agent on an on-premises server. This allows you to run Code Deploy builds on that server. Code Deploy can be linked into ECR which would allow us to deploy from within the AWS infrastructure without polling on the server.

This means that there has to be a path from the public internet where ECR and Code Deploy run to the on-premises server which is a security risk. Because this is a custodial copy, we want the server to be decoupled from AWS as much as possible. Installing the agent introduces tight coupling with our AWS infrastructure.

#### A custom deployment pipeline

A pipeline triggered from ECS builds and SQS
An event is published from ECR when a new image is pushed. This can be configured in Event Bridge to push to an SQS queue. We could then have a custom script which listens to the SQS queue and then redeploys the image when the message is received. This custom code and associated AWS infrastructure would have to be written and maintained within the team. The Event Bridge messages are also published on a “best effort” basis which may mean we miss an update. The SQS queue also introduces another security risk.

#### A pipeline triggered on a schedule

This avoids the problems with securing SQS queues and potentially missing updates to the image but it still requires custom code which will have to be maintained within the team.

## Consequences

### Advantages

- Automated Image Updates: Watchtower is a container update and management tool that monitors the ECR registry for changes. It automatically detects when new Docker images are available and initiates the update process on the on-premises server. This ensures that our Docker containers are always up to date with the latest versions.
- Simplified Deployment Process: Watchtower eliminates the need for manual intervention in the deployment process. It continuously scans the ECR registry, identifies the updated images, pulls them, and replaces the running containers on the on-premises server. This streamlines the deployment process, saving time and reducing the risk of human errors.
- Rollback Support: Watchtower provides the ability to roll back to previous versions of Docker images if any issues arise after deployment. This ensures that we can quickly revert to a known working state in case of compatibility or stability issues with the updated images.
- Fine-Grained Update Control: Watchtower offers flexibility in managing updates by supporting various update strategies, such as scheduling updates during specific maintenance windows or defining update policies based on tags or labels associated with Docker images. This allows us to have control over when and how updates are applied.
- Security and Compliance: Watchtower supports image signature verification, ensuring the integrity and authenticity of the Docker images being deployed from ECR. This helps maintain a secure and compliant deployment process by preventing the deployment of tampered or unauthorised images.

### Disadvantages

- Additional Component: Introducing Watchtower adds an extra component to the deployment infrastructure. This requires monitoring, maintenance, and potential updates of the Watchtower tool itself.
- Dependency on Internet Connectivity: Watchtower relies on an internet connection to communicate with the ECR registry and retrieve updated Docker images. Any disruptions in the internet connectivity may impact the timely deployment of new images to the on-premises server.

### Conclusion

By choosing Watchtower as the tool for deploying Docker images from AWS ECR to our on-premises server, we can: automate the update process, simplify deployment, and ensure that our containers are consistently up to date. We accept the trade-offs of: introducing an additional component and dependency on internet connectivity, considering the benefits of automated updates, rollback support, fine-grained control, and security features provided by Watchtower.
