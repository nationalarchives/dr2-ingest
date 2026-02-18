# DRI Migration

This directory contains a SQL query used to extract basic provenance metadata out of the DRI Preservica database.

There is also a Python script to run the query and write the results to a JSON file in S3 which adheres to the format
needed by the package builder lambda.

We aren't sending real files into the DR2 ingest at the moment so the script takes the skeleton test file from DROID
which corresponds to the PUID of the file we're migrating.

## Running the script.

This needs to be run on a Web domain machine.

### Create a tunnel

#### Optional - configure ssh

In a file `$HOME/.ssh/config` add this

```
Host *
HostKeyAlgorithms +ssh-rsa
MACs +hmac-sha1
```

Make sure the file is saved as UTF-8 otherwise ssh will complain.

### Create a tunnel

Open a Powershell or bash window. Run

```
ssh -L 9998:dri-oracle1.uat:22 -N spalmer@dri-mgmt1.dri.web.local
```

Then in another tab

```
ssh -L 1521:localhost:1521 -N -p 9998 spalmer@localhost
```

### Set up Python script environment

* Download and extract the “Basic Light
Package” [for Windows](https://www.oracle.com/uk/database/technologies/instant-client/winx64-64-downloads.html)
or [for Linux](https://www.oracle.com/uk/database/technologies/instant-client/linux-x86-64-downloads.html).

* Set up a Python environment. You can install Pycharm or
use [a virtual environment](https://docs.python.org/3/library/venv.html). There are other methods as well.

* Once you have an environment, install boto3 and oracledb from pip.

* Add AWS credentials to the environment, either through environment variables or the $HOME/.aws/credentials file

* Clone [DROID](https://github.com/digital-preservation/droid) onto the machine

* Set the [environment variables](#environment-variables) as described below.

* Set the WHERE clause in the script to match the files you want to migrate. For example:

Migrate a single file
```sql
WHERE du.CATALOGUEREFERENCE = 'TEST/123/AB/Z'
```

Migrate a whole series

```sql
WHERE du.CATALOGUEREFERENCE LIKE 'TEST/123%'
```

* Run the Python script.

### Environment variables

| Name            | Description                                                                                                         |
|-----------------|---------------------------------------------------------------------------------------------------------------------|
| CLIENT_LOCATION | The path to the Oracle Instant Client directory.                                                                    |
| STORE_PASSWORD  | The password to connect to the database.                                                                            |
| DROID_PATH      | The path to the `droid-core/test-skeletons/` directory in the DROID repo                                            |
| ACCOUNT_NUMBER  | The DR2 account number of the environment you are ingesting to                                                      |
| ENVIRONMENT     | The DR2 environment name of the environment you are ingesting to                                                    |
| TEST_RUN        | Set to anything other than 'true' to do a live run. This is optional and 'test_run' will default to True if omitted |

### Script steps
1. Connect to the database using the credentials and client location provided in the environment variables.
2. For each file in the database that matches the WHERE clause:
   - Get the file metadata from the database.
   - If this is a test run, get the path of the skeleton file from DROID otherwise get the path to the real file.
   - If this is a test run generate checksums for the skeleton file for each original algorithm in Preservica.
   - If the file is a redacted version (with a `type_ref` of 100), append `rel_ref - 1` to the `FileReference field.
   - Write the metadata and file to S3 in a JSON format suitable for the package builder lambda.
   - Send an SQS message to the <env>-dr2-preingest-dri-importer queue to trigger the copy lambda.
