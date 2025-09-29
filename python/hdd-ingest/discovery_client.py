import re
from urllib.parse import urlencode, quote

import requests

BASE_URL = "This needs to come from some config?"

def get_record_collection(citable_reference):
    operation = "records/v1/collection/{query}"
    param = {"query": citable_reference}
    query_encoded = quote(param["query"])
    url = f"{BASE_URL}/{operation.format(query=query_encoded)}"

    response = requests.get(url)
    return response

def get_description(citable_reference):
    response = get_record_collection(citable_reference)
    if response.status_code == 200:
        data = response.json()
        # description includes HTML tags that we need to strip
        description_html = data.get("assets")[0].get("scopeContent").get("description")
        return re.sub(r"<.*?>", "", description_html)
    else:
        raise Exception(f"Unable to get description. Received: {response.status_code}, {response.text}")


def get_title(citable_reference):
    collection_response = get_record_collection(citable_reference)
    if collection_response.status_code == 200:
        data = collection_response.json()
        # description includes HTML tags that we need to strip
        record_id = data.get("assets")[0].get("id")

        details_operation =  "records/v1/details/{id}"
        param = {"id": record_id}
        query_encoded = quote(param["id"])
        url = f"{BASE_URL}/{details_operation.format(id=query_encoded)}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json().get("title")
        else:
            raise Exception(f"Unable to get title. Received: {response.status_code}, {response.text}")

    else:
        raise Exception(f"Unable to get record collection. Received: {collection_response.status_code}, {collection_response.text}")

def main():
    print(get_title("JS 8/4"))

if __name__ == "__main__":
    main()