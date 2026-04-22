import base64
import hashlib
import os
import sys
from multiprocessing import Pool
import boto3

path = sys.argv[1]
files = os.listdir(path)
client = boto3.client("s3")
version = sys.argv[2]
bucket = "mgmt-dp-code-deploy"

def upload_file(file):
    with open(f"{path}/{file}", "rb") as f:
        digest = hashlib.sha256(f.read()).digest()
        sha256 = base64.b64encode(digest).decode()
        client.put_object(Bucket=bucket, Key=f"{version}/{file}", Body=f, ChecksumSHA256=sha256)
        print(f"Uploaded {file} to {bucket}")

with Pool(5) as p:
    p.map(upload_file, files)



