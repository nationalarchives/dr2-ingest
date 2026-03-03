# Generate restore package

This is a script which is run on the dri-docker1.<env> servers.
Given a catalogue reference, it extracts the files and XML metadata for an OCFL object from the repository and writes it
to an output directory.

The script does not send this to S3. This has to be done later through the CLI.

The script has no dependencies. It's difficult to install dependencies on those servers so this only uses imports from
the Python standard library.

## Steps

* Query the DB builder database for the object id and source id for the code provided in the argument.
* Get the root path for the OCFL object.
* Get the file paths from the `inventory.json` file in the OCFL object.
* Read the IO metadata file and extract the list of content object ids.
* For each content object id, add the metadata from the CO_Metadata file to a list and copy the file to the output
  directory.
* Concatenate the `IO_Metadata` and list of `CO_Metadata` to a new file in the output directory.

## Running the script

```bash
OUTPUT_DIRECTORY=/path/to/output/ DB_PATH=/path/to/builder.db REPO_PATH=/path/to/ocfl/repo python3 generate_restore_files.py ABC/D/1
```

## Environment variables

| Name             | Description                                      |
|------------------|--------------------------------------------------|
| OUTPUT_DIRECTORY | The directory to write the files and metadata to |
| DB_PATH          | The path to the builder database.                |
| REPO_PATH        | The path to OCFL repository repo directory       |

## Starting the ingest
The source bucket for the restore preingest is encrypted with a KMS key which is accessible only by a specific role.
This role needs to be assumed before running these commands. 

```bash
#!/bin/bash
# Set credentials
aws sts assume-role --role-arn arn:aws:iam::<account-number>:role/<env>-dr2-ingest-cc-restore-role --role-session-name cc-restore | jq > tmp-creds
export AWS_ACCESS_KEY_ID=$(cat tmp-creds| jq -r '.Credentials.AccessKeyId')
export AWS_SECRET_ACCESS_KEY=$(cat tmp-creds | jq -r '.Credentials.SecretAccessKey')
export AWS_SESSION_TOKEN=$(cat tmp-creds | jq -r '.Credentials.SessionToken')
rm tmp-creds

# Sync to S3
aws s3 sync /path/to/output/ s3://<env>-dr2-ingest-cc-restore-cache

# Send SQS message. You will need the asset id which is the name of the subfolder in the output directory
aws sqs send-message --queue-url https://sqs.eu-west-2.amazonaws.com/059334750967/intg-dr2-preingest-restore-importer --message-body '{"assetId":"<asset_id>","bucket":"<env>-dr2-ingest-cc-restore-cache"}'
```