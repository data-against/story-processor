import logging

import requests
import requests.exceptions

from processor import NEWSCATCHER_API_KEY

# Setup a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def search_stories(session, params):
    """
    Fetch stories from the Newscatcher API given the session and parameters.

    Args:
        session (requests.Session): requests session
        params (dict): The query parameters to send to the API

    Returns:
        dict: The JSON response containing the stories if successful, otherwise a trivial dict.
    """
    try:
        response = session.get(
            "https://v3-api.newscatcherapi.com/api/search",
            headers={"x-api-token": NEWSCATCHER_API_KEY},
            params=params,
        )

        # Check if the response is successful
        if response.status_code == 200:
            return response.json()
        else:
            # Log an error and return an empty dictionary if the request failed
            logger.error(
                f"Failed to fetch stories. Status code: {response.status_code} - {response.reason} - {response.text}"
            )
            return dict(total_hits=0)

    except requests.exceptions.RequestException as re:
        logger.error(f"Request exception occurred: {re}")
        return dict(total_hits=0)
