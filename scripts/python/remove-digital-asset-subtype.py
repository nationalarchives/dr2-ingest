# This script makes the DigitalAssetSubtype of assets to be empty where the value is 'TDR'. This was needed as
# we had assets coming from 'TDR', that had been ingested using a digital asset subtype of 'TDR'
#
# Implementation: The script makes use of pyPreservica library to connect to Preservica and sequentially invoke
# various APIs to get the desired effect. It effectively works in the steps as:
# 1) Receive a consignment reference as a parameter on command line
# 2) Based on the consignment reference, get hold of the asset identifiers in it
# 3) Get the asset entity from the identifiers
# 4) Find metadata for the asset entity
# 5) Extract the `<Source>` element as string from the metadata response
# 6) Modify the `<Source>` element to change the DigitalAssetSubtype to be empty
# 7) Invoke the PUT method for the same metadata URL with the modified XML
#
# Usage: The script can be run in a python environment by passing parameters on the command line
#     e.g. Usage: python3 remove-digital-asset-subtype.py '<username>' '<password>' '<consignment_reference>'
#     Note: Sometimes the password may contain special characters which are interpreted differently by the shell,
#           in such cases, it is important to enclose the parameters in single quotes.
#

import pyPreservica
import sys
import requests

# Note: The following url is a placeholder, Change it to be the correct url corresponding to your environment
SERVER = "tna.preservica.com"
UPSTREAM_SYSTEM_REF = "<UpstreamSystemRef>"

if len(sys.argv) < 4:
    print("Usage: python3 remove-digital-asset-subtype.py '<username>' '<password>' '<consignment_reference>'")
    print("Note: Make sure to enclose the parameters in single quotes to avoid problems with characters like $")
    sys.exit(1)
username = sys.argv[1]
password = sys.argv[2]
consignment_reference = sys.argv[3]

entity_client = pyPreservica.EntityAPI(username, password, None, SERVER)
assets = entity_client.identifier("ConsignmentReference", consignment_reference)
for asset in assets:
    print(f"Processing: {asset.reference}")
    asset_entity = entity_client.asset(asset.reference)
    metadata_urls = asset_entity.metadata.keys()
    for metadata_url in metadata_urls:
        metadata_response_text = requests.get(metadata_url, headers={"Preservica-Access-Token": f"{entity_client.token}"}).text

        # ==== Locate the Source fragment by finding the starting and ending tag in the metadata response
        source_element_start_index = metadata_response_text.find("<Source")
        if source_element_start_index == -1:
            print(f"Source element not found in the metadata xml for {asset.reference}")
            continue
        source_element_end_index = metadata_response_text.find("</Source>", source_element_start_index)
        source_fragment = metadata_response_text[source_element_start_index: source_element_end_index + len("</Source>")]

        # ==== Modify the extracted fragment and invoke a PUT request
        if UPSTREAM_SYSTEM_REF in source_fragment:
            modified_source_fragment = source_fragment.replace(UPSTREAM_SYSTEM_REF, '')
            put_response = requests.put(metadata_url, modified_source_fragment, headers={"Content-Type":"application/xml", "Preservica-Access-Token": f"{entity_client.token}"})
            if put_response.status_code == 200:
                print(f"DigitalAssetSubtype for {asset.reference} successfully removed")
            else:
                print(f"DigitalAssetSubtype update did not succeed for {asset.reference} due to {put_response.text}")
        else:
            print(f"DigitalAssetSubtype for {asset.reference} is not 'TDR', nothing to change.")