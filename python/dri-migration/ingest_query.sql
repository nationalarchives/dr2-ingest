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
LENGTH(REPLACE(REGEXP_SUBSTR(du.CATALOGUEREFERENCE, '^([A-Z]{1,}\/[0-9]{1,}|HGD[0-9]{1,}_FX)'), '/', ' ')) series2,
    REPLACE(REGEXP_SUBSTR(du.CATALOGUEREFERENCE, '^([A-Z]{1,}\/[0-9]{1,}|HGD[0-9]{1,}_FX)'), '/', ' ') series,
    df.FILEREF AS FILEID,
    du.DELIVERABLEUNITREF AS UUID,
    du.DESCRIPTION,
    a.DATE_ transferInitiatedDateTime,
    duParent.CATALOGUEREFERENCE PARENTCATALOGREFERENCE,
    df.NAME fileName,
    f.FIXITIES,
    du.CATALOGUEREFERENCE AS fileReference,
    file_location.file_path,
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
WHERE du.CATALOGUEREFERENCE LIKE 'RG/140%'