from unittest import TestCase
from unittest.mock import patch, MagicMock

import requests
import discovery_client

class Test(TestCase):

    @patch("requests.get")
    def test_should_return_description_as_received_from_discovery(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"assets": [{"scopeContent": {"placeNames": [], "description": "<scopecontent><p>Some long description for testing.</p></scopecontent>"}}]}
        mock_request.return_value = mock_response

        description = discovery_client.get_description("AB 1/2")
        self.assertEqual("Some long description for testing.", description)

    @patch("requests.get")
    def test_should_raise_an_exception_when_unable_to_find_description_from_discovery(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {"error": "Not found"}
        mock_response.text = "Not found"
        mock_request.return_value = mock_response

        with self.assertRaises(Exception) as e:
            discovery_client.get_description("AB 1/2")

        self.assertEqual("Unable to get description. Received: 404, Not found", str(e.exception))

    @patch("requests.get")
    def test_should_return_title_as_received_from_discovery(self, mock_request):
        mock_response_collection = MagicMock()
        mock_response_collection.status_code = 200
        mock_response_collection.json.return_value = {"assets": [{"id": "known_id"}]}

        mock_response_title = MagicMock()
        mock_response_title.status_code = 200
        mock_response_title.json.return_value = {"title": "title of record"}

        def mock_side_effects(url, *args, **kwargs):
            if url.endswith("records/v1/collection/AB%201/2"):
                return mock_response_collection
            elif url.endswith("records/v1/details/known_id"):
                return mock_response_title
            else:
                self.fail("error in test, investigate further")

        mock_request.side_effect = mock_side_effects

        title = discovery_client.get_title("AB 1/2")
        self.assertEqual("title of record", title)

    @patch("requests.get")
    def test_should_throw_error_when_collection_succeeds_but_title_retrieval_fails(self, mock_request):
        mock_response_collection = MagicMock()
        mock_response_collection.status_code = 200
        mock_response_collection.json.return_value = {"assets": [{"id": "known_id"}]}

        mock_response_title = MagicMock()
        mock_response_title.status_code = 404
        mock_response_title.json.return_value = {"error": "nothing to see here"}
        mock_response_title.text = "Nothing to see here"

        def mock_side_effects(url, *args, **kwargs):
            if url.endswith("records/v1/collection/AB%201/2"):
                return mock_response_collection
            elif url.endswith("records/v1/details/known_id"):
                return mock_response_title
            else:
                self.fail("error in test, investigate further")

        mock_request.side_effect = mock_side_effects

        with self.assertRaises(Exception) as e:
            discovery_client.get_title("AB 1/2")

        self.assertEqual("Unable to get title. Received: 404, Nothing to see here", str(e.exception))

