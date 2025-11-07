WITH FIXITIES AS (
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
    REPLACE(REGEXP_SUBSTR(du.CATALOGUEREFERENCE, '^([A-Z]{1,}\/[0-9]{1,}|HGD[0-9]{1,}_FX)'), '/', ' ') series,
    df.FILEREF AS FILEID,
    du.DELIVERABLEUNITREF AS UUID,
    TO_CLOB(REGEXP_SUBSTR(x.XMLCLOB, '<dcterms:description xmlns:dcterms="http://purl.org/dc/terms/" rdf:datatype="xs:string">(.*?)</dcterms:description>', 1, 1, NULL, 1)) AS DESC1,
    TO_CLOB(REGEXP_SUBSTR(x.XMLCLOB, '<dc:description xmlns:dc="http://purl.org/dc/terms/" rdf:datatype="xs:string">(.*?)</dc:description>', 1, 1, NULL, 1)) AS DESC2,
    a.DATE_ transferInitiatedDateTime,
    duParent.CATALOGUEREFERENCE PARENTCATALOGREFERENCE,
    df.NAME fileName,
    f.FIXITIES,
    REGEXP_REPLACE(du.CATALOGUEREFERENCE, '^([A-Z]{1,}\/[0-9]{1,}|HGD[0-9]{1,}_FX)\/', '') AS fileReference,
    file_location.file_path,
    '/dri/' || fs_location.path || '/' || file_location.file_path AS fullPath,
    x.XMLCLOB AS METADATA,
    fp.PUID AS PUID,
CAST( REGEXP_SUBSTR(x.XMLCLOB, '<tna:batchIdentifier rdf:datatype="xs:string">(.*?)</tna:batchIdentifier>', 1 , 1, NULL, 1) AS VARCHAR(200)) AS DRIBATCHREFERENCE,
CAST(REGEXP_SUBSTR(x.XMLCLOB, '<tna:tdrConsignmentRef rdf:datatype="xs:string">(.*?)</tna:tdrConsignmentRef>', 1, 1, NULL, 1) AS VARCHAR(200)) AS CONSIGNMENTREFERENCE,
CAST(CAST(REGEXP_SUBSTR(x.XMLCLOB, '<tna:ordinal rdf:datatype="xs:decimal">(.*?)</tna:ordinal>', 1, 1, NULL, 1) AS VARCHAR(200)) AS NUMBER) AS SORTORDER,
dum.MANIFESTATIONRELREF,
dum.TYPEREF,
du.SECURITYTAG
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
    WHERE du.CATALOGUEREFERENCE LIKE 'MH/12/%'
    AND du.DELIVERABLEUNITREF NOT IN ('f1fce216-dd8e-4b1e-81f2-2cdbee1da12d','e995247a-2e04-4063-98b0-982a29fbf904')