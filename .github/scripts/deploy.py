import boto3
import sys
import json

environment = sys.argv[1]
version = sys.argv[2]
deploy_bucket = "mgmt-dp-code-deploy"

s3 = boto3.client("s3", region_name="eu-west-2")
aws_lambda = boto3.client("lambda", region_name="eu-west-2")
eventbridge = boto3.client("events", region_name="eu-west-2")


def send_slack_message(slack_message: str):
    detail = json.dumps({"slackMessage": slack_message})
    eventbridge.put_events(
        Entries=[{'Source': 'DR2IngestDeploy', 'DetailType': 'DR2DevMessage', 'Detail': detail}]
    )


def send_failure_slack_message(key):
    send_slack_message(
        f":alert-noflash-slow: Deploy for *{key}* with version *{version}* on environment *{environment}* failure"
    )


def update_functions():
    objects = s3.list_objects(Bucket=deploy_bucket, Prefix=version)
    print(f"Found {len(objects)} objects")
    keys = [obj["Key"].replace(f"{version}/", "") for obj in objects["Contents"]]
    if environment != "intg":
        keys = [k for k in keys if k != 'court-document-package-anonymiser']
            
    successful_slack_message = f"These are the successful deploys to version *{version}* on environment *{environment}*:\n\n"
    for key in keys:
        print(f"Processing key {key}")
        try:
            aws_lambda.update_function_code(FunctionName=f"{environment}-dr2-{key}", S3Bucket=deploy_bucket,
                                            S3Key=f"{version}/{key}")
            successful_slack_message += f":green-tick: {key}\n"
        except Exception as err:
            print(err)
            send_failure_slack_message(key)
            exit(1)
    try:
        send_slack_message(successful_slack_message)
    except Exception as err:
        print(err)
        exit(1)


try:
    update_functions()
except Exception as err:
    print(err)
    send_failure_slack_message("unknown-lambda")
    exit(1)
