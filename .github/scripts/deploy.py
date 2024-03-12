import boto3
import sys
import json

environment = sys.argv[1]
version = sys.argv[2]
deploy_bucket = "mgmt-dp-code-deploy"

s3 = boto3.client("s3", region_name="eu-west-2")
aws_lambda = boto3.client("lambda", region_name="eu-west-2")
eventbridge = boto3.client("events", region_name="eu-west-2")


def send_slack_message(key, icon, status):
    detail = json.dumps(
        {"slackMessage": f"{icon} Deploy for {key} with version {version} on environment {environment} {status}"})
    eventbridge.put_events(
        Entries=[{'Source': 'DR2IngestDeploy', 'DetailType': 'DR2DevMessage', 'Detail': detail}]
    )


def update_functions():
    objects = s3.list_objects(Bucket=deploy_bucket, Prefix=version)
    print(f"Found {len(objects)} objects")
    keys = [obj["Key"].replace(f"{version}/", "") for obj in objects["Contents"]]
    for key in keys:
        print(f"Processing key {key}")
        aws_lambda.update_function_code(FunctionName=f"{environment}-{key}", S3Bucket=deploy_bucket, S3Key=f"{version}/{key}")
        send_slack_message(key, ":green-tick:", "successful")
        try:
            aws_lambda.update_function_code(FunctionName=key, S3Bucket=deploy_bucket, S3Key=key)
            send_slack_message(key, ":green-tick:", "successful")
        except Exception as err:
            print(err)
            send_slack_message(key, ":alert-noflash-slow:", "failure")
            exit(1)


try:
    update_functions()
except Exception as err:
    print(err)
    send_slack_message("unkown-lambda", ":alert-noflash-slow:", "failure")
    exit(1)
