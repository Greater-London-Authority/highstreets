import base64
import logging
import os

import requests
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class APIClient:
    token_endpoint = "https://api.business.bt.com/oauth/accesstoken"  # noqa: S105

    @staticmethod
    def get_access_token():
        consumer_key = os.getenv("CONSUMER_KEY")
        consumer_secret = os.getenv("CONSUMER_SECRET")

        # Encode consumer key and secret as base64
        auth_header = base64.b64encode(
            f"{consumer_key}:{consumer_secret}".encode()
        ).decode()

        # Set headers for Basic Authentication
        headers = {"Authorization": f"Basic {auth_header}"}

        # Set the necessary parameters
        params = {"grant_type": "client_credentials"}

        try:
            # Make a POST request to the token endpoint
            response = requests.get(
                APIClient.token_endpoint, headers=headers, params=params, timeout=5
            )
            # Raise an exception if status code indicates an error
            response.raise_for_status()

            # Parse the response and extract the token
            token = response.json().get("accessToken")

            logger.info("Access token obtained successfully.")
            return token
        except requests.exceptions.RequestException as e:
            logger.error("Failed to obtain access token: %s", str(e))
            raise
