import hashlib
import os
import uuid
from pathlib import Path, PureWindowsPath, PurePosixPath

import discovery_client

def create_intermediate_metadata_dict(row, args):
    catalog_ref = row["catRef"].strip()
    file_path = row["fileName"].strip()
    collection_info = discovery_client.get_title_and_description(catalog_ref)
    former_references = discovery_client.get_former_references(collection_info.identifier)

    if not collection_info.title and not collection_info.description:
        raise Exception(f"Title and Description both are empty for '{catalog_ref}', unable to proceed with this record")

    description_to_use = collection_info.title if collection_info.title is not None else collection_info.description
    series = catalog_ref.split("/")[0].strip()
    metadata = {
        "Series": series,
        "UUID": str(uuid.uuid4()),
        "fileId": str(uuid.uuid4()),
        "description": description_to_use,
        "Filename": get_filename_from_cross_platform_path(file_path),
        "FileReference": catalog_ref.removeprefix(f"{series}").strip().removeprefix("/").strip(),
        "ClientSideOriginalFilepath": file_path,
    }
    former_ref_dept = former_references.formerRefDept
    metadata["formerRefDept"] = "" if former_ref_dept is None else former_ref_dept

    former_ref_tna = former_references.formerRefTNA
    metadata["formerRefTNA"] = "" if former_ref_tna is None else former_ref_tna

    sha256_checksum = row["checksum"].strip()
    if not sha256_checksum:
        metadata["checksum_md5"] = create_md5_hash(get_absolute_file_path(args.input, file_path))
        metadata["checksum_sha256"] = ""
    else:
        metadata["checksum_md5"] = ""
        metadata["checksum_sha256"] = sha256_checksum
    return metadata

def create_metadata_for_upload(row):
    metadata = {
        "Series": row["Series"],
        "UUID": row["UUID"],
        "fileId": row["fileId"],
        "description": row["description"],
        "Filename": row["Filename"],
        "FileReference": row["FileReference"],
        "ClientSideOriginalFilepath": row["ClientSideOriginalFilepath"]
    }
    if row["formerRefDept"] != "":
        metadata["formerRefDept"] = row["formerRefDept"]
    if row["formerRefTNA"] != "":
        metadata["formerRefTNA"] = row["formerRefTNA"]
    if row["checksum_sha256"] == "":
        metadata["checksum_md5"] = row["checksum_md5"]
    else:
        metadata["checksum_sha256"] = row["checksum_sha256"]
    return metadata

def get_field_names():
    return ["Series", "UUID", "fileId", "description", "Filename", "FileReference",
                  "ClientSideOriginalFilepath", "formerRefDept", "formerRefTNA", "checksum_md5", "checksum_sha256"]

def get_absolute_file_path(input_path, relative_or_absolute_file_path):
    input_file_path = Path(input_path).resolve()

    if Path(relative_or_absolute_file_path).is_absolute():
        return str(relative_or_absolute_file_path)
    else:
        normalised_relative_path = os.path.normpath(relative_or_absolute_file_path.replace("\\", "/"))
        full_path = Path(input_file_path.parent / normalised_relative_path).resolve()
        return str(full_path)

def create_md5_hash(file_path, chunk_size=8192):
    md5 = hashlib.md5()
    with open(file_path, "rb") as the_file:   # open in binary mode
        for chunk in iter(lambda: the_file.read(chunk_size), b""):
            md5.update(chunk)
    return md5.hexdigest()

def get_filename_from_cross_platform_path(path_str):
    if "\\" in path_str or ":" in path_str: #maybe Windows path
        return PureWindowsPath(path_str).name.strip()
    else :
        return PurePosixPath(path_str).name.strip()

