import re
from dataclasses import dataclass
from urllib.parse import quote

import requests

@dataclass
class CollectionInfo:
    identifier: str
    title: str | None
    description: str | None

@dataclass
class RecordDetails:
    formerRefDept: str | None
    formerRefTNA: str | None

BASE_URL = "https://discovery.nationalarchives.gov.uk/API/"
REC_COLLECTION_OPERATION = "records/v1/collection/{query}"
REC_DETAILS_OPERATION = "records/v1/details/{query}"

def get_response_from_operation(operation, query_param):
    param = {"query": query_param}
    query_encoded = quote(param["query"])
    url = f"{BASE_URL}/{operation.format(query=query_encoded)}"

    response = requests.get(url)
    return response

# def get_record_details(identifier):
#     return get_response_from_operation(REC_DETAILS_OPERATION, identifier)
#
# def get_record_collection(citable_reference):
#     operation = "records/v1/collection/{query}"
#     param = {"query": citable_reference}
#     query_encoded = quote(param["query"])
#     url = f"{BASE_URL}/{operation.format(query=query_encoded)}"
#
#     response = requests.get(url)
#     return response

def get_former_references(identifier):
    response = get_response_from_operation(REC_DETAILS_OPERATION, identifier)
    if response.status_code == 200:
        details = response.json()
        former_ref_dept = details.get("formerReferenceDep")
        former_ref_tna = details.get("formerReferencePro")

        return RecordDetails(former_ref_dept, former_ref_tna)
    else:
        raise Exception(f"Unable to get title or description. Received: {response.status_code}, {response.text}")

def get_title_and_description(citable_reference):
    response = get_response_from_operation(REC_COLLECTION_OPERATION, citable_reference)
    if response.status_code == 200:
        data = response.json()
        assets = data.get("assets")
        if not assets:
            return "", ""
        first_asset = assets[0]
        # description includes HTML tags that we need to strip
        description_html = first_asset.get("scopeContent").get("description")
        description = re.sub(r"<.*?>", "", description_html)
        title = first_asset.get("title")
        identifier = first_asset.get("id")
        return CollectionInfo(identifier, title, description)
    else:
        raise Exception(f"Unable to get title or description. Received: {response.status_code}, {response.text}")


def is_discovery_api_reachable():
    operation = "records/v1/collection/JS"
    url = f"{BASE_URL}/{operation}"
    response = requests.get(url)
    return response.status_code == 200


def main():
    print("Error: Only expected to use as a module")

if __name__ == "__main__":
    main()