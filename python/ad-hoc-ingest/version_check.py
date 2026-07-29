import hashlib
import json
import os
import urllib.request
from pathlib import Path

REPO_API_URL = (
    "https://api.github.com/repos/nationalarchives/dr2-ingest"
    "/contents/python/ad-hoc-ingest"
)


def fetch_json(url: str) -> list:
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.loads(response.read())


def fetch_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read()


def md5_of_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def fetch_remote_files() -> list[dict]:
    entries = fetch_json(REPO_API_URL)
    return [e for e in entries if e["type"] == "file" and "_test" not in e["name"]]


def is_latest_version() -> bool:
    remote_files = fetch_remote_files()
    assert remote_files
    adhoc_file_path = [os.path.join(dp, f) for dp, dn, filenames in os.walk(".") for f in filenames if f == "ad_hoc_ingest.py"][0]
    local_dir = Path(adhoc_file_path).parent
    local_files = [d for d in os.listdir(local_dir) if "_test" not in d]

    for entry in remote_files:
        name = entry["name"]
        download_url = entry["download_url"]

        if name not in local_files:
            return False

        remote_md5 = md5_of_bytes(fetch_bytes(download_url))
        local_md5 = md5_of_bytes((Path(local_dir) / Path(name)).read_bytes())
        if remote_md5 != local_md5:
            return False
    return True
