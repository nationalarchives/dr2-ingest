# Ad hoc Ingest

This directory contains the scripts used for ingesting the data from ad hoc sources such as hard drives that 
The National Archives has received over the years. The scripts and process strictly applies to the data 
which meets the following criteria:
- The data has been already received and held on some storage media 
- The data is NOT ingested into the existing preservation system
- The data has been already available to the public on Discovery.

## The process:
![adhoc-ingest-flow.png](images/adhoc-ingest-flow.png)

There is a script (ad_hoc_ingest.py) at the centre of the ingest process which drives the generic ingest. The script receives a CSV file as an input. 
This input CSV file contains the filenames, catalog reference, a checksum and any other information. The files are 
located in a folder or other media which can be made accessible to the script. The script works chronologically in steps 
as shown in the diagram.


### Script steps
1. Read the input CSV file, where each row in the CSV has Catlog Reference, Filename and a Checksum corresponding to each file to be ingested. 
2. Read the files to be ingested from ad-hoc source and carry out validations on the input file.
   - The validations depend on Series to be ingested. 
For each row in the input CSV file,
3. Use Discovery API to get hold of the Title, Description and Former References.
4. Generate metadata and add it as a row to intermediate metadata CSV file.
   - The advantage of having metadata in CSV is, the user can examine the potential final metadata before ingesting. 
For each row in the Metadata CSV file,
5. Generate a Metadata JSON file and get absolute path of the file to be ingested. 
6. Upload the Metadata JSON and the file to `dr2-ingest-raw-cache` bucket. 
7. Send a message to `dr2-preingest-adhoc-importer` to trigger the downstream preingest process.


## How to run the script:
The script needs to connect to Discovery API to get some metadata for the records. It also needs to connect to 
AWS services to upload each file and its metadata into S3 bucket and send a message to `dr2-preingest-adhoc-importer` queue 
to trigger the downstream process. As a result, the script needs to be run from a machine which has access 
to the internet by a user who has permissions to access the relevant AWS services. 

### Script Arguments 

`-i   --input`
    A CSV (or Excel) file having rows corresponding to the data to be ingested. Each row has a Catalog Reference (catRef), name of the file (fileName) with absolute path, and optionally, a checksum for the file to be ingested

`-e  --environment`
    Name of the environment where the records are ingested (e.g. intg, prod). The script makes use of the environment name to construct names of the AWS resources (default 'intg')

`-d   --dry_run`
    Boolean value, when True, indicates that the script should only validate the data but stop short of actually uploading it to S3. False indicates that the script should ingest data as well. (default False)

`-o   --output` 
    Optional file path where the intermediate results are stored. When available, the output of dry run is stored in that file. If not given, the script generates a file in the "tmp" folder of the machine


