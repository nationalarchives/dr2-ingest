# This script updates the DigitalAssetSubtype of assets that have already been ingested. This was needed as
# we had assets coming from 'TDR' had been ingested using a digital asset subtype of 'TDR'
#
# Implementation: The script makes use of pyPreservica library to connect to Presevica and sequentially invoke
# various APIs to get the desired effect. It effectively works in the steps as
# 1) Receive a consignment reference as a parameter on command line
# 2) Based on the consignment reference, get hold of the asset identifiers in it
# 3) Get the asset entity from the identifiers
# 4) Find metadata for the asset entity
# 5) If the DigitalAssetSubtype in the metadata XML is 'TDR', make that element empty
# 6) Modify the XML to strip out namespace prefixes from everything other than the 'Source' element
# 7) Call the update_metadata and pass in the modified XML
#
# Usage: The script can be run in a python environment by passing parameters on the command line
#     e.g. Usage: python3 remove-digital-asset-subtype.py '<username>' '<password>' '<consignment_reference>'
#     Note: Sometimes the password may contain special characters which are interpreted differently by the shell,
#           in such cases, it is important to enclose the parameters in single quotes.
#

import pyPreservica
import xml.etree.ElementTree
import re
import sys

SOURCE_NAMESPACE = "http://dr2.nationalarchives.gov.uk/source"

if len(sys.argv) < 4:
    print("Usage: python3 remove-digital-asset-subtype.py '<username>' '<password>' '<consignment_reference>'")
    print("Note: Make sure to enclose the parameters in single quotes to avoid problems with characters like $")
    sys.exit(1)

username = sys.argv[1]
password = sys.argv[2]
consignment_reference = sys.argv[3]

# Change the server url to be the correct one corresponding to your environment
entity_client = pyPreservica.EntityAPI(username, password, None, "demo.example.com")
assets = entity_client.identifier("ConsignmentReference", consignment_reference)
for asset in assets:
    print(f"processing {asset.reference}")
    asset_entity = entity_client.asset(asset.reference)
    met = entity_client.metadata_for_entity(asset, SOURCE_NAMESPACE)
    xml_fragment = xml.etree.ElementTree.fromstring(met)
    namespace = {'ns0': SOURCE_NAMESPACE}
    old_subtype = xml_fragment.find('ns0:DigitalAssetSubtype', namespaces=namespace)
    if old_subtype is not None and old_subtype.text == "TDR":
        old_subtype.text = None
        #trim whitespaces immediately after '>' in tags
        modified_xml_string = re.sub(r">\s+", ">", xml.etree.ElementTree.tostring(xml_fragment, encoding="utf-8").decode()).strip()

        #removenamespace prefix, otherwise this appears on the UI
        modified_xml_string = modified_xml_string.replace("<ns0:", "<").replace("</ns0:", "</")

        #however, the top element 'Source' need to be prefixed with the namespace otherwise we get parsing error
        modified_xml_string = modified_xml_string.replace("<Source", "<ns0:Source").replace("</Source", "</ns0:Source")
        entity_client.update_metadata(asset_entity, SOURCE_NAMESPACE, modified_xml_string)
        print(f"{asset.reference} Done")
    else:
        print(f"Old subtype for '{asset.reference}' is not 'TDR', it is '{old_subtype}'")