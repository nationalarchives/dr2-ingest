e2e tests 

# How to run e2e tests locally 
To run the e2e tests locally, you will need to have the following environment variables set:
- `ADHOC_BUCKET` - The S3 bucket name used for adhoc uploads
- `ADHOC_SQS_QUEUE` - The URL of SQS queue for adhoc importer
- `COPY_ADHOC_FILES_LOG_GROUP` - The ARN of CloudWatch log group of adhoc importer lambda 
- `COPY_DRI_FILES_LOG_GROUP` - The ARN of CloudWatch log group for DRI importer lambda
- `COPY_FILES_LOG_GROUP` - The ARN of CloudWatch log group for TDR importer lambda
- `DRI_BUCKET` - The S3 bucket name used for DRI uploads
- `DRI_SQS_QUEUE` - The URL of SQS queue for DRI importer
- `EXTERNAL_LOG_GROUP` - The ARN of CloudWatch log group for external notifications
- `INGEST_SQS_QUEUE` - The URL of SQS queue for TDR importer
- `JUDGMENT_SQS_QUEUE` - The URL of SQS queue for Judgment importer
- `LOCK_TABLE` - The name of DynamoDB lock table
- `PREINGEST_SFN_ARN` - The ARN of the preingest Step Function
- `S3_BUCKET` - The S3 bucket name used for preingest 

If you wish to run it on commandline, in the terminal window, 
- Export the above environment variables with correct values for the environment where you wish the e2e tests to run
- Assume the e2e test role in the environment where you wish to run the tests.
  (This may involve you giving yourself permissions first to assume the e2e role.)

Once set, you can navigate to the e2etests directory and run the tests using the following command:
```bash
./gradlew clean test
```
Please note, the tests will run in parallel by default, so any console output may be interleaved.

If you wish to run it from an IDE, 
- you can set the environment variables in the run configuration. 
- Assume the e2e test role in the environment where you wish to run the tests. 
  (This may involve you giving yourself permissions first to assume the e2e role.)
- You can then run the configuration in IDE
Please note, the IDE runner may run the tests sequentially, so it is likely to take a lot longer if you run the entire test suite in IDE. 