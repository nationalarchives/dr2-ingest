# DR2 Ingest Lambdas

This is a monorepo which contains [multiple sbt subprojects](https://www.scala-sbt.org/1.x/docs/Multi-Project.html). 
Each subproject is a different lambda.

## Deployment
When a change is made to one subproject and merged to main, **all** lambas are deployed to the environment.
This means that all lambdas are on the same version number. 

## Creating a new Lambda.

There is a `LambdaRunner` trait in the utils package. 
You will need to extend this and implement the required methods.

You will also need three or four case classes.

### Input
This is the object representing the lambda input json. The `LambdaRunner` trait needs an implicit circe decoder for this class.

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