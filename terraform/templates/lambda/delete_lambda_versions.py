import boto3
from datetime import datetime, timezone, timedelta

from botocore.exceptions import ClientError

client = boto3.client("lambda")

def list_all_functions():
    functions = []
    paginator = client.get_paginator("list_functions")
    for page in paginator.paginate():
        functions.extend(page["Functions"])
    return functions

def list_all_versions(function_name):
    versions = []
    paginator = client.get_paginator("list_versions_by_function")
    for page in paginator.paginate(FunctionName=function_name):
        versions.extend(page["Versions"])
    return versions


def is_sfn_function(function_arn):
    tags = client.list_tags(Resource=function_arn)['Tags']
    return 'SfnFunction' in tags and tags['SfnFunction'] == 'true'

def lambda_handler(event, context):
    functions = list_all_functions()
    for function in functions:
        if is_sfn_function(function['FunctionArn']):
            function_name = function['FunctionName']
            versions = list_all_versions(function_name)
            named_versions = [v for v in versions if v['Version'] != '$LATEST']
            named_versions_length = len(named_versions)
            if named_versions_length > 3:
                to_delete = named_versions[:(named_versions_length - 3)]
                for version_to_delete in to_delete:
                    lmd = datetime.strptime(version_to_delete['LastModified'], "%Y-%m-%dT%H:%M:%S.%f%z")
                    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
                    if lmd < cutoff:
                        try:
                            client.delete_function(FunctionName=function_name, Qualifier=version_to_delete['Version'])
                        except ClientError as e:
                            print(e)
                            continue
