import json
import sqlite3
from pathlib import Path

import pytest

import generate.generate_restore_package as restore


def _make_db_with_code(mapping):
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute("create table files (code text, id text, sourceId text)")
    for code, ids in mapping.items():
        for id_mapping in ids:
            for object_id, source_id in id_mapping.items():
                cur.execute("insert into files(code, id, sourceId) values(?, ?, ?)", (code, object_id, source_id))
    conn.commit()
    cur.close()
    return conn


def _write_inventory(root_path: Path, file_paths):
    inv = {
        "head": "v1",
        "manifest": {
            "abcdefg": file_paths
        },
        "versions": {
            "v1": {
                "state": {
                    "abcdefg": []
                }
            }
        },
    }
    (root_path / "inventory.json").write_text(json.dumps(inv), encoding="utf-8")


def _write_io_metadata(root_path: Path, rel_path: str, content_object_ids):
    xml = (
        '<XIP xmlns="http://preservica.com/XIP/v7.7">'
        '<xip:InformationObject xmlns="http://preservica.com/EntityAPI/v7.7" '
        'xmlns:xip="http://preservica.com/XIP/v7.7">'
        '<xip:Representation xmlns="http://preservica.com/EntityAPI/v7.7" '
        'xmlns:xip="http://preservica.com/XIP/v7.7">'
        "<xip:ContentObjects>"
        + "".join(f"<xip:ContentObject>{cid}</xip:ContentObject>" for cid in content_object_ids)
        + "</xip:ContentObjects>"
        "</xip:Representation>"
        "</xip:InformationObject>"
        "</XIP>"
    )
    p = root_path / rel_path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(xml, encoding="utf-8")


def test_get_ocfl_root_path_uses_sha256_and_repo_path():
    object_id = "abc"
    sha = __import__("hashlib").sha256(object_id.encode()).hexdigest()
    expected = f"/repo/{sha[0:3]}/{sha[3:6]}/{sha[6:9]}/{sha}"
    assert restore.get_ocfl_root_path("/repo", object_id) == expected


def test_get_file_paths_reads_inventory_json(tmp_path):
    root = tmp_path / "obj"
    root.mkdir()
    file_paths = ["a/b.txt", "c/d.txt"]
    _write_inventory(root, file_paths)
    assert sorted(restore.get_file_paths(str(root))) == sorted(file_paths)


def test_get_content_object_ids_parses_ids(tmp_path):
    root = tmp_path / "obj"
    root.mkdir()
    rel = "v1/content/IO_Metadata.xml"
    ids = ["id1", "id2"]
    _write_io_metadata(root, rel, ids)

    got = restore.get_content_object_ids(str(root), rel)
    assert got == ids


def test_get_id_no_rows_exits(capsys):
    conn = _make_db_with_code({"CODE1": []})
    with pytest.raises(SystemExit) as e:
        restore.get_ids(conn, "CODE1")
    assert e.value.code == 1
    out = capsys.readouterr().out
    assert "No rows found for code CODE1" in out


def test_get_id_multiple_rows_exits(capsys):
    conn = _make_db_with_code({"CODE1": [{"idA":"sourceA"}, {"idB": "sourceB"}]})

    with pytest.raises(SystemExit) as e:
        restore.get_ids(conn, "CODE1")
    assert e.value.code == 1
    out = capsys.readouterr().out
    assert "More than one ID found for code CODE1" in out


def test_get_id_single_row_returns_id():
    conn = _make_db_with_code({"CODE1": [{"idA":"sourceA"}]})
    assert restore.get_ids(conn, "CODE1") == {'object_id': 'idA', 'source_id': 'sourceA'}


def test_run_copies_expected_files(tmp_path):
    object_id = "obj-123"
    source_id = "source-123"
    conn = _make_db_with_code({"TESTCODE": [{object_id: source_id}]})


    repo = tmp_path / "repo"
    out = tmp_path / "out"
    repo.mkdir()
    out.mkdir()

    ocfl_root = Path(restore.get_ocfl_root_path(repo, object_id))
    ocfl_root.mkdir(parents=True, exist_ok=True)

    co_ids = ["co1", "co2"]
    io_rel = f"{object_id}/IO_Metadata.xml"

    file_paths = [io_rel]
    for co_id in co_ids:
        meta_rel = f"{object_id}/Preservation_1/{co_id}/CO_Metadata.xml"
        file_rel = f"{object_id}/Preservation_1/{co_id}/b.bin"
        file_paths.extend([meta_rel, file_rel])

        (ocfl_root / meta_rel).parent.mkdir(parents=True, exist_ok=True)
        (ocfl_root / meta_rel).write_text(f"<meta>{co_id}</meta>", encoding="utf-8")
        (ocfl_root / file_rel).write_bytes(f"DATA-{co_id}".encode("utf-8"))

    _write_inventory(ocfl_root, file_paths)
    _write_io_metadata(ocfl_root, io_rel, co_ids)

    restore.run(conn, repo, out, "TESTCODE")

    assert (out / source_id / "co1").exists()
    assert (out / f"{source_id}.metadata").exists()

    assert (out / source_id / "co2").read_bytes() == b"DATA-co2"
    assert (out / f"{source_id}.metadata").read_text(encoding="utf-8").startswith("<XIP")