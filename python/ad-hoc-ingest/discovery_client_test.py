from unittest import TestCase
from unittest.mock import patch, MagicMock

import discovery_client


class Test(TestCase):

    @patch("requests.get")
    def test_should_return_title_and_description_as_received_from_discovery(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"assets": [{"scopeContent": {"description": "Some long description for testing.", "placeNames": []},"title": "some_title"}]}
        mock_request.return_value = mock_response

        collection_info = discovery_client.get_title_and_description("AB 1/2")
        self.assertEqual("Some long description for testing.", collection_info.description)
        self.assertEqual("some_title", collection_info.title)

    @patch("requests.get")
    def test_should_return_title_and_description_after_stripping_html_tags_from_response_received_from_discovery(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"assets": [{"scopeContent": {"description": "<scopecontent><p><list><item>Test Description Number &#49;</item></list></p></scopecontent>", "placeNames": []},"title": "<unittitle>This title uses &#x31; hex encoded value</unittitle>"}]}
        mock_request.return_value = mock_response

        collection_info = discovery_client.get_title_and_description("AB 1/2")
        self.assertEqual("Test Description Number 1", collection_info.description)
        self.assertEqual("This title uses 1 hex encoded value", collection_info.title)

    @patch("requests.get")
    def test_should_return_empty_title_description_when_assets_cannot_be_found(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"no_assets": [{"scopeContent": {"description": "<scopecontent><p><br>Some long description for testing.</p></scopecontent>", "placeNames": []},"title": "some_title"}]}
        mock_request.return_value = mock_response

        collection_info = discovery_client.get_title_and_description("AB 1/2")
        self.assertEqual("", collection_info.iaid)
        self.assertEqual(None, collection_info.description)
        self.assertEqual(None, collection_info.title)

    @patch("requests.get")
    def test_should_raise_an_exception_when_unable_to_find_title_or_description_from_discovery(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {"error": "Not found"}
        mock_response.text = "Not found"
        mock_request.return_value = mock_response

        with self.assertRaises(Exception) as e:
            discovery_client.get_title_and_description("AB 1/2")

        self.assertEqual("Unable to get title or description. Received: 404, Not found", str(e.exception))

    @patch("requests.get")
    def test_should_return_former_references_as_received_from_discovery(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"arrangement": "<arrangement>Original file path: d:\\js\\3\\1\\evid0003.pdf</arrangement>","batchId": "ero","copiesInformation": [],"corporateNames": [],"creatorName": [],"formerReferenceDep": "former-dep-ref:0001","formerReferencePro": "AB 3/2/1","immediateSourceOfAcquisition": [],"language": "English","legalStatus": "Public Record(s)","physicalDescriptionForm": "digital record(s)","scopeContent": {"placeNames": [],"description": "<scopecontent><p>Abstract of evidence by some council.</p></scopecontent>","ephemera": None,"schema": None},"digitised": True,"heldBy": [{"xReferenceURL": None,}],"id": "C1234567","source": None,"title": None}
        mock_request.return_value = mock_response

        record_details = discovery_client.get_former_references("C1234567")
        self.assertEqual("former-dep-ref:0001", record_details.formerRefDept)
        self.assertEqual("AB 3/2/1", record_details.formerRefTNA)

    @patch("requests.get")
    def test_should_return_former_references_as_none_when_it_does_not_exist(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"arrangement": "<arrangement>Original file path: d:\\js\\3\\1\\evid0003.pdf</arrangement>","batchId": "ero","copiesInformation": [],"corporateNames": [],"creatorName": [],"formerReferenceDep": None,"formerReferencePro": "AB 3/2/1","immediateSourceOfAcquisition": [],"language": "English","legalStatus": "Public Record(s)","physicalDescriptionForm": "digital record(s)","scopeContent": {"placeNames": [],"description": "<scopecontent><p>Abstract of evidence by some council.</p></scopecontent>","ephemera": None,"schema": None},"digitised": True,"heldBy": [{"xReferenceURL": None,}],"id": "C1234567","source": None,"title": None}
        mock_request.return_value = mock_response

        record_details = discovery_client.get_former_references("C1234567")
        self.assertEqual(None, record_details.formerRefDept)
        self.assertEqual("AB 3/2/1", record_details.formerRefTNA)

