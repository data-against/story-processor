import unittest
from unittest.mock import Mock, patch

import requests

from scripts.newscatcher_api import search_stories


class TestSearchStories(unittest.TestCase):

    @patch("requests.Session.get")
    def test_search_stories_success(self, mock_get):
        # mock a successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "total_hits": 100,
            "articles": [{"title": "Article 1", "author": "Riley Smith"}],
        }
        mock_get.return_value = mock_response

        session = requests.Session()
        params = {"q": "test"}

        result = search_stories(session, params)

        self.assertEqual(result["total_hits"], 100)
        self.assertEqual(len(result["articles"]), 1)
        self.assertEqual(result["articles"][0]["title"], "Article 1")

    @patch("requests.Session.get")
    def test_search_stories_failure(self, mock_get):
        # simulate a failed API response
        mock_response = Mock()
        mock_response.status_code = 422
        mock_response.reason = "Unprocessable Entity Parameter is not allowed"
        mock_response.text = "Param langs is not allowed for plan nlp"
        mock_get.return_value = mock_response

        session = requests.Session()
        params = {"q": "test"}

        result = search_stories(session, params)

        # make sure we get an empty dictionary on failure
        self.assertEqual(result, {})

    @patch("requests.Session.get")
    def test_search_stories_exception(self, mock_get):
        # simulate a RequestException being raised
        mock_get.side_effect = requests.exceptions.RequestException("Connection error")

        session = requests.Session()
        params = {"q": "test"}

        result = search_stories(session, params)

        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
