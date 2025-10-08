# Digital Records Repository (DR2)

Digital Records Repository (DR2) is the Preservation Service for digital and digitised records within The National Archives. Digital records are stored, actively preserved, and accessed using a commercial off-the-shelf digital preservation managed service. Around this service, we have built a control mechanism to facilitate ingest of and access to digital objects.

![Diagram of DR2 components in AWS](/docs/images/dr2-end-to-end.svg)

This project consists of multiple repositories:

- [dr2-preservica-client](https://github.com/nationalarchives/dr2-preservica-client) contains a wrapper library for the Preservica API, designed to abstract Preservation System specific features from our business logic.
- [nationalarchives/dr2-custodial-copy](https://github.com/nationalarchives/dr2-custodial-copy) contains the source code for the Custodial Copy Service. A service designed to run on-premise as an extracted decoupled copy of all digital files and metadata.
- [nationalarchives/dr2-ingest](https://github.com/nationalarchives/dr2-ingest) (this one) contains the source code for the Lambda functions within DR2.
- [nationalarchives/dr2-terraform-environments](https://github.com/nationalarchives/dr2-terraform-environments) contains the Terraform infrastructure as code for our AWS environment.

For documentation, see [`docs/README.md`](./docs/README.md).

## dr2-ingest

This is a monorepo which contains the source code for the Lambda functions within DR2. We have adopted a practice of using the approapiate language for the task, hence this repository contains Lambda functions of multiple flavours.

For our Scala Lambdas, this repository contains [multiple sbt subprojects](https://www.scala-sbt.org/1.x/docs/Multi-Project.html) where each subproject is a different Lambda.

## Deployment
When a change is made to one subproject and merged to main, **all** lambdas are deployed to the environment.
This means that all lambdas are on the same version number. 

## Creating a new Lambda.

There is a `LambdaRunner` abstract class in the utils package. 
You will need to extend this and implement the required methods.

You will also need three or four case classes.

### Input
This is the object representing the lambda input json. The `LambdaRunner` abstract class needs an implicit circe decoder for this class.

For a case class, you can import `io.circe.generic.auto._` which will provide the required decoder. 

For the AWS Java classes like `SQSEvent`, there are circe decoders in the `EventDecoders` class in the utils package.

### Output
This is optional. If you want the lambda to write data to the output stream, provide a case class here and it will be encoded to json and written to the output stream.
You will need to provide a circe encoder to do this. This can be provided using `io.circe.generic.auto._` 

If you don't want the lambda to return any output, you can use `Unit` here.

### Config
The config case class. This is passed to the `handler` method and the `dependencies` method used to instantiate dependencies.

### Dependencies
A case class containing external classes the lambda needs to run, for example, Preservica clients or AWS clients. 


```scala
import io.circe.generic.auto._
import pureconfig.generic.auto._ //Needed for decoding the config

class Lambda extends LambdaRunner[Input, Output, Config, Dependencies] {
  override def handler: (
    SQSEvent,
      Config,
      Dependencies
    ) => IO[Unit] = (input, config, dependencies) => for {
    _ <- logger.info(s"Starting lambda with inputValue ${input.inputValue}") //logger provided by the trait
    _ <- dependencies.daSNSClient.publish[String](config.snsArn)("message")
  } yield Output("returnValue")
}
object Lambda {
  case class Input(inputValue: String)
  
  case class Config(preservicaUrl: String, secretName: String, otherConfig: Int)
  
  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], daSNSClient: DASNSClient[IO], daDynamoDBClient: DADynamoDBClient[IO])
  
  case class Output(valueToBeReturned: String)
}
```

## Testing
You can test the lambdas by passing in mock clients to the handler method.

```scala
"handler" should "do something" in {
  val input = Input("some input")
  val config = Config("http://preservica", "secretName", "something else")
  val entityClient = mock[EntityClient[IO, Fs2Streams[IO]]]
  val daSNSClient: DASNSClient[IO] = mock[DASNSClient[IO]] 
  val daDynamoDBClient: DADynamoDBClient[IO] = mock[DADynamoDBClient[IO]]
  val dependencies = Dependencies(entityClient, daSNSClient, daDynamoDBClient)
  val output = new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
  output.valueToBeReturned should equal("something")
}
```

## Deployment
To include the newly created lambda for deployment with all the existing lambdas, you can add the name of newly created lambda to `build.sbt` found under `<project_root>scala/lambdas/build.sbt` as shown below
    
```scala
lazy val ingestLambdasRoot = (project in file("."))
  .aggregate(
    existingLambda1, // One or more existing lambdas
    existingLambda2,
    existingLambda3,
    newLambda  // Add the name of the new lambda here
  )
```
For the automatic deployment to succeed, you also need to update the `deploy_lambda_policy`. Update [terraform code](https://github.com/nationalarchives/dr2-terraform-environments/blob/main/deploy_roles.tf#L22) corresponding to the policy and add this lambda into the list of existing lambdas 
