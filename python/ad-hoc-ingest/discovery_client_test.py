from unittest import TestCase
from unittest.mock import patch, MagicMock

import discovery_client


class Test(TestCase):

    @patch("requests.get")
    def test_should_return_title_and_description_as_received_from_discovery(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"assets": [{"scopeContent": {"placeNames": [], "description": "<scopecontent><p><br>Some long description for testing.</p></scopecontent>", "title": None}}]}
        mock_request.return_value = mock_response

        title, description = discovery_client.get_title_and_description("AB 1/2")
        self.assertEqual("Some long description for testing.", description)
        self.assertEqual(None, title)

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

