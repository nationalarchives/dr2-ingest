import argparse
import os

import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError

MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", 20))


def iter_s3_keys(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            yield key


def copy_to_destination_prefix(s3_client, bucket, source_key, destination_prefix):
    filename = source_key.rsplit("/", 1)[-1]
    destination_key = destination_prefix.rstrip("/") + "/" + filename
    copy_source = {"Bucket": bucket, "Key": source_key}

    s3_client.copy_object(
        Bucket=bucket,
        CopySource=copy_source,
        Key=destination_key,
    )


def main(series_list):
    s3 = boto3.client("s3")

    copied = 0
    failed = []
    for series in series_list:
        bucket = "prod-dr2-records-metadata"
        source_prefix = f"working/{series.strip()}/"
        destination_prefix = "live/"
        try:
            with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
                futures = {
                    executor.submit(copy_to_destination_prefix, s3, bucket, source_key, destination_prefix): source_key
                    for source_key in iter_s3_keys(s3, bucket, source_prefix)
                }

                for future in as_completed(futures):
                    source_key = futures[future]
                    try:
                        future.result()
                        copied += 1
                        if copied % 1000 == 0:
                            print(f"Copied {copied}")
                    except ClientError as e:
                        print(f"Failed to copy {source_key}: {e}")
                        failed.append(source_key)

        except ClientError as e:
            print(f"Error listing objects: {e}")
            raise

    print(f"\nDone. Copied {copied} objects, {len(failed)} failed.")
    if len(failed) > 0:
        with open("failed.txt", "w") as failed_file:
            failed_file.writelines(failed)
        print(f"Failed keys have been written to {os.getcwd()}/failed.txt")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("series", help="A comma separated list of series to copy")
    args = parser.parse_args()
    main(args.series.split(","))
