import getpass
import pyPreservica
import requests

CLOSING_SOURCE_TAG = "</Source>"
CLOSING_SOURCE_TAG_LEN = len(CLOSING_SOURCE_TAG)
DIGITAL_ASSET_SUBTYPE_EMPTY = "<DigitalAssetSubtype></DigitalAssetSubtype>"
DIGITAL_ASSET_SUBTYPE_TDR = "<DigitalAssetSubtype>TDR</DigitalAssetSubtype>"

def remove_digital_asset_subtype(username, password, consignment_reference, server):
    entity_client = pyPreservica.EntityAPI(username, password, None, server)
    assets = entity_client.identifier("ConsignmentReference", consignment_reference)
    for asset in assets:
        print(f"Processing: {asset.reference}")
        asset_entity = entity_client.asset(asset.reference)
        metadata_urls = asset_entity.metadata.keys()
        for metadata_url in metadata_urls:
            metadata_response_text = requests.get(metadata_url,
                                                  headers={"Preservica-Access-Token": f"{entity_client.token}"}).text

            # ==== Locate the Source fragment by finding the starting and ending tag in the metadata response
            source_element_start_index = metadata_response_text.find("<Source")
            if source_element_start_index == -1:
                print(f"Source element not found in the metadata xml for {asset.reference}")
                continue
            source_element_end_index = metadata_response_text.find(CLOSING_SOURCE_TAG, source_element_start_index)
            source_fragment = metadata_response_text[
                source_element_start_index: source_element_end_index + CLOSING_SOURCE_TAG_LEN
            ]

            # ==== Modify the extracted fragment and invoke a PUT request
            if DIGITAL_ASSET_SUBTYPE_TDR in source_fragment:
                modified_source_fragment = source_fragment.replace(DIGITAL_ASSET_SUBTYPE_TDR, DIGITAL_ASSET_SUBTYPE_EMPTY)
                put_response = requests.put(metadata_url, modified_source_fragment,
                                            headers={"Content-Type": "application/xml",
                                                     "Preservica-Access-Token": f"{entity_client.token}"})
                if put_response.status_code == 200:
                    print(f"DigitalAssetSubtype for {asset.reference} successfully removed")
                else:
                    print(f"DigitalAssetSubtype update did not succeed for {asset.reference} due to {put_response.text}")
            else:
                print(f"DigitalAssetSubtype for {asset.reference} is not 'TDR', nothing to change.")

def main():
    """
     This script makes the DigitalAssetSubtype of assets to be empty where the value is 'TDR'. This was needed as
     we had assets coming from 'TDR', that had been ingested using a digital asset subtype of 'TDR'

     Implementation: The script makes use of pyPreservica library to connect to Preservica and sequentially invoke
     various APIs to get the desired effect. It effectively works in the steps as:
     1) Receive a consignment reference as a parameter on command line
     2) Based on the consignment reference, get hold of the asset identifiers in it
     3) Get the asset entity from the identifiers
     4) Find metadata for the asset entity
     5) Extract the `<Source>` element as string from the metadata response
     6) Modify the `<Source>` element to change the DigitalAssetSubtype to be empty
     7) Invoke the PUT method for the same metadata URL with the modified XML

     Usage: The script can be run in a python environment by passing parameters on the command line
         e.g. Usage: python3 remove-digital-asset-subtype.py

    """
    # Note: The following url is a placeholder, Change it to be the correct url corresponding to your environment
    SERVER = "demo.example.com"

    username = input("Username: ").strip()
    # If script is run via an IDE, an "GetPassWarning" will appear, informing you that password might be echoed
    password = getpass.getpass(prompt="Password: ").strip()
    consignment_reference = input("Consignment Reference: ").strip()
    remove_digital_asset_subtype(username, password, consignment_reference, SERVER)

if __name__ == "__main__":
    main()
