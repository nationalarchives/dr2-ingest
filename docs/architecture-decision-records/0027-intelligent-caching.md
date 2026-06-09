# 27. Custodial Copy Intelligent Caching

**Date:** 2026-06-04

## Context
As we [migrate from DRI](/python/dri-migration/README.md), we will be uploading files into our Preservation System (DR2) from Kew. Custodial Copy (CC),
in its current state, downloads the file from the Preservation System and store it in Kew which, for the files we 
want to migrate from DRI, wastes resources, time, and adds extra cost and time since the files are already in Kew.

## Decision

We would want to establish the following arrangement: files that need to end up being stored in Kew will be held in a storage
location (folder) in Kew. They might be held in arbitrary folder structures within this location.

When CC is about to download a file from the Preservation System, it will check this local cache to see if the exact file 
is already available there, and sources it locally if so, otherwise, it will source it from the Preservation System; 
files could be added to, or removed from, the cache by other parties/systems. We would want to begin the upload and 
ingest process from this cache location also.

Due to the simplicity of this approach, the size of the information in the cache and no concurrent writes needed, 
we will use a SQLite DB. We can't put the DB in the DA since it's possible that continuous writes could cause a lot of copies
to be written to tape, therefore we need to create a new NFS share on filers that can be accessed from PRD and UAT.
We can set it to be backed up daily as the costs to set up or run would be low. The format of the table will be like so:


| file_id (type text)                  | file_path (type text) | asset_id (type text)                 |
|--------------------------------------|-----------------------|--------------------------------------|
| 738c55cf-cfa1-4f57-98c9-c585577b9916 | path/to/local/file1   | 24fce28a-4605-4071-922b-f70ab12bcbe4 |
| f07e6bb6-74b1-4607-af70-93bb6045d716 | path/to/local/file2   | 24fce28a-4605-4071-922b-f70ab12bcbe4 |


### Implementation

- In the DRI migration script, after the writing of the results to JSON files in S3, we will add code that writes the
  (local) file path, file id and asset id of each asset to a SQLite database (the path of which would be passed in
    when the script is run).
- In CC, for a given file, right before it downloads the file from the Preservation System, it will use the file id in 
  order to call the SQLite database table and retrieve the file's local file path.
- It will use the file path to download the file to the location where Preservation System downloads go to and then 
  continue with the other processes.


#### Considerations

The migration script could fail while the DB table is being written to and meaning the script would have to be rerun.
`file_id` is a unique field so if on a rerun, it tries to add a row with a `file_id` that is already present, then an
error will be raised; we can mitigate this by adding an "ON CONFLICT"-like restriction.

