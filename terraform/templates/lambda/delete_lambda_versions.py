import boto3
from datetime import datetime, timezone, timedelta

client = boto3.client("lambda")

def lambda_handler(event, context):
    functions = client.list_functions()['Functions']
    for function in functions:
        function_name = function['FunctionName']
        versions = client.list_versions_by_function(FunctionName=function_name)['Versions']
        named_versions = [v for v in versions if v['Version'] != '$LATEST']
        named_versions_length = len(named_versions)
        if named_versions_length > 3:
            to_delete = named_versions[:(named_versions_length - 3)]
            for version_to_delete in to_delete:
                lmd = datetime.strptime(version_to_delete['LastModified'], "%Y-%m-%dT%H:%M:%S.%f%z")
                cutoff = datetime.now(timezone.utc) - timedelta(days=30)
                if lmd < cutoff:
                    client.delete_function(FunctionName=function_name, Qualifier=version_to_delete['Version'])
