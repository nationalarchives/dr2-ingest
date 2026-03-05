import hashlib
import json
import pathlib
import shutil
import sqlite3
import os
import sys
import xml.etree.ElementTree as ET


def get_ids(conn, code):
    cur = conn.cursor()
    cur.execute("select id, sourceId from files where code = ? group by 1, 2", (code,))
    row = cur.fetchall()
    cur.close()
    if len(row) == 0:
        print(f"No rows found for code {code}")
        exit(1)
    elif len(row) > 1:
        print(f"More than one row found for code {code}")
        exit(1)
    else:
        return {'object_id': row[0][0], 'source_id': row[0][1]}


def get_ocfl_root_path(repo_path, object_id):
    sha256 = hashlib.sha256(object_id.encode()).hexdigest()
    path_parts = []
    for i in range(0, 9, 3):
        part = sha256[i: i + 3]
        path_parts.append(part)
    path_parts.append(sha256)
    path = "/".join(path_parts)
    return f"{repo_path}/{path}"


def get_content_object_ids(root_path, io_metadata_path, xip_version):
    with open(f"{root_path}/{io_metadata_path}", "r") as io_metadata_file:
        io_metadata_xml = io_metadata_file.read()
    root = ET.fromstring(io_metadata_xml)
    ns = {"xip": f"http://preservica.com/XIP/{xip_version}"}

    return [e.text for e in root.findall(".//xip:ContentObjects/xip:ContentObject", ns)]


def get_file_paths(root_path):
    with open(f"{root_path}/inventory.json", "r") as f:
        inventory = json.load(f)
        head = inventory["head"]
        paths = []
        for sha in inventory["versions"][head]["state"].keys():
            paths.extend(inventory["manifest"][sha])
    return paths


def generate_co_xml(path, xip_version):
    with open(path) as co_file:
        f = co_file.read()
        co_elem = ET.fromstring(f)
    ns = {"xip": f"http://preservica.com/XIP/{xip_version}"}
    co_root = ET.Element("CCContentObject")

    content_objects = co_elem.findall("./xip:ContentObject", ns)
    bitstreams_object = co_elem.findall("./xip:Generation/xip:Bitstreams", ns)
    bitstream_object = co_elem.findall("./xip:Bitstream", ns)
    co_root.extend(content_objects)
    co_root.extend(bitstreams_object)
    co_root.extend(bitstream_object)
    return co_root

def run(conn, repo_path, output_directory, code, xip_version):
    ids = get_ids(conn, code)
    object_id = ids['object_id']
    source_id = ids['source_id']
    root_path = get_ocfl_root_path(repo_path, object_id)
    file_paths = get_file_paths(root_path)
    io_metadata_path = [x for x in file_paths if x.endswith("IO_Metadata.xml")][0]
    content_object_ids = get_content_object_ids(root_path, io_metadata_path, xip_version)
    pathlib.Path(f"{output_directory}/{source_id}").mkdir(parents=True, exist_ok=True)
    cc_cos = ET.Element("CCContentObjects")

    for co_id in content_object_ids:
        co_files = sorted([x for x in file_paths if f"{object_id}/Preservation_1/{co_id}" in x])
        co_metadata_path = [f for f in co_files if f.endswith("CO_Metadata.xml")][0]
        file_path = [f for f in co_files if not f.endswith("CO_Metadata.xml")][0]
        shutil.copyfile(f"{root_path}/{file_path}", f"{output_directory}/{source_id}/{co_id}")
        cc_co_elem = generate_co_xml(f"{root_path}/{co_metadata_path}", xip_version)
        cc_cos.append(cc_co_elem)

    io_full_path = f"{root_path}/{io_metadata_path}"
    with open(io_full_path, "r") as r:
        metadata_file = r.read()
        all_metadata = metadata_file.replace('</XIP>', ET.tostring(cc_cos).decode("utf-8") + '</XIP>')
    with open(f"{output_directory}/{source_id}.metadata", "w") as w:
        w.write(all_metadata)


if __name__ == "__main__":
    db_path = os.environ['DB_PATH']
    ocfl_repo_path = os.environ['REPO_PATH']
    output_dir = os.environ['OUTPUT_DIRECTORY']
    run(sqlite3.connect(db_path), ocfl_repo_path, output_dir, sys.argv[1], sys.argv[2])