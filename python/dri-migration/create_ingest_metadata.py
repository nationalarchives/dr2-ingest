import io
import json
import re
from os import listdir
import oracledb
import os
import boto3

oracledb.init_oracle_client(lib_dir=os.environ['CLIENT_LOCATION'])
conn = oracledb.connect(dsn='localhost/SDB4', user="STORE", password=os.environ['STORE_PASSWORD'])
cur = conn.cursor()

puid_lookup = {}


def create_skeleton_suite_lookup(prefix):
    path = f"{os.environ['DROID_PATH']}\\{prefix}"

    pattern = re.compile(r'((x-)?fmt-\d{1,5})-signature-id-\d{1,5}(\.[A-Za-z]{1,10})')

    directory_list = listdir(path)

    for name in directory_list:
        match = pattern.search(name)
        if match:
            puid = match.group(1).replace('fmt-', 'fmt/')
            puid_lookup[puid] = {'file_path': f"{path}\\{name}"}


create_skeleton_suite_lookup('fmt')
create_skeleton_suite_lookup('x-fmt')

sql = r"""WITH FIXITIES AS (
SELECT
    d.FILEREF,
    '[' || LISTAGG('{"' || f.ALGORITHMNAME || '":"' || d.FIXITYVALUE || '"}', ',') WITHIN GROUP (ORDER BY f.ALGORITHMNAME) || ']' AS fixities
FROM
    DIGITALFILEFIXITYINFO d
JOIN FIXITYALGORITHM f ON
    f.FIXITYALGORITHMREF = d.FIXITYALGORITHMREF
GROUP BY
    d.FILEREF
HAVING
    COUNT(*) > 1
),
FIRSTPUID AS (
    SELECT FILEREF, MAX(FORMATPUID) AS PUID FROM DIGITALFILEFORMAT GROUP BY FILEREF
)
SELECT
    REGEXP_SUBSTR(du.CATALOGUEREFERENCE, '^([A-Z]{1,}\/[0-9]{1,}|HGD[0-9]{1,}_FX)') series,
    df.FILEREF AS FILEID,
    du.DELIVERABLEUNITREF AS UUID,
    du.DESCRIPTION,
    a.DATE_ transferInitiatedDateTime,
    duParent.CATALOGUEREFERENCE consignmentReference,
    df.NAME fileName,
    f.FIXITIES,
    du.CATALOGUEREFERENCE AS fileReference,
    '/dri/' || fs_location.path || '/' || file_location.file_path AS fullPath,
    x.XMLCLOB AS METADATA,
    fp.PUID AS PUID,
    COALESCE(
CAST( REGEXP_SUBSTR(x.XMLCLOB, 'xsi:type="tnacdc:batchIdentifier">(.*?)</dc:isPartOf>', 1 , 1, NULL, 1) AS VARCHAR(200) ),
CAST(REGEXP_SUBSTR(x.XMLCLOB, '<tna:tdrConsignmentRef rdf:datatype="xs:string">(.*?)</tna:tdrConsignmentRef>', 1, 1, NULL, 1) AS VARCHAR(200))
) AS CONSIGNMENTREFERENCE
FROM
    DIGITALFILE df
JOIN file_location ON
    file_location.file_ref = df.FILEREF
JOIN fs_location_file_locations ON
    file_location.file_location_id = fs_location_file_locations.file_locations
JOIN FIXITIES f ON f.FILEREF = df.FILEREF
JOIN fs_location ON
    fs_location_file_locations.stored_file_set_locations = fs_location.ID
JOIN INGESTEDFILESET ifs ON
    ifs.INGESTEDFILESETREF = df.INGESTEDFILESETREF
JOIN ACCESSION a ON
    a.ACCESSIONREF = ifs.ACCESSIONREF
JOIN XMLMETADATA x ON
    x.METADATAREF = df.METADATAREF
LEFT JOIN MANIFESTATIONFILE mf ON
    df.FILEREF = mf.FILEREF
LEFT JOIN DELIVERABLEUNITMANIFESTATION dum ON
    mf.MANIFESTATIONREF = dum.MANIFESTATIONREF
LEFT JOIN DELIVERABLEUNIT du ON
    dum.DELIVERABLEUNITREF = du.DELIVERABLEUNITREF
LEFT JOIN DELIVERABLEUNIT duParent ON
    duParent.DELIVERABLEUNITREF = du.TOPLEVELREF
    LEFT JOIN FIRSTPUID fp ON fp.FILEREF = df.FILEREF
WHERE du.CATALOGUEREFERENCE LIKE 'RG/140%'"""
cur.execute(sql)

bucket = "intg-dr2-ingest-raw-cache"

page_size = 100

client = boto3.client("s3")

key_indexes = {}
for idx, keys in enumerate(cur.description):
    key_indexes[keys[0]] = idx

while True:
    rows = cur.fetchmany(page_size)
    if not rows:
        break
    for row in rows:
        puid = row[key_indexes["PUID"]]
        asset_uuid = row[key_indexes["UUID"]]
        full_path = row[key_indexes["FULLPATH"]]
        checksum_data = json.loads(row[key_indexes["FIXITIES"]])
        metadata = {
            "Series": row[key_indexes["SERIES"]],
            "UUID": asset_uuid,
            "fileId": row[key_indexes["FILEID"]],
            "description": row[key_indexes["DESCRIPTION"]],
            "TransferInitiatedDatetime": str(row[key_indexes["TRANSFERINITIATEDDATETIME"]]),
            "ConsignmentReference": row[key_indexes["CONSIGNMENTREFERENCE"]],
            "Filename": row[key_indexes["FILENAME"]],
            "fileReference": row[key_indexes["FILEREFERENCE"]],
            "metadata": str(row[key_indexes["METADATA"]]),
            "originalPath": full_path
        }
        for each_checksum in checksum_data:
            for algorithm, fingerprint in each_checksum.items():
                metadata[f"checksum_{algorithm.lower().replace("-", "")}"] = fingerprint
        # client.upload_file(full_path, bucket, file_uuid) # This won't work but you get the idea.
        client.upload_file(puid_lookup[puid]['file_path'], bucket, asset_uuid)

        json_bytes = io.BytesIO(json.dumps(metadata).encode("utf-8"))
        client.upload_fileobj(json_bytes, bucket, f"{asset_uuid}.metadata")
