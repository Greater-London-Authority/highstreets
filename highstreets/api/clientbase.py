import base64
import logging
import os
import uuid

import requests
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class APIClientException(Exception):
    pass


class APIClient:
    token_endpoint = "https://api.business.bt.com/oauth/accesstoken"  # noqa: S105

    def __init__(self):
        self.token = self.get_access_token()

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
            response = requests.get(  # noqa: S113
                APIClient.token_endpoint, headers=headers, params=params  # noqa: S113
            )
            # Raise an exception if status code indicates an error
            response.raise_for_status()

            # Parse the response and extract the token
            token = response.json().get("accessToken")

            logger.info("Access token obtained successfully.")
            return token
        except requests.exceptions.RequestException as e:
            logger.error("Failed to obtain access token: %s", str(e))
            raise APIClientException("Failed to obtain access token.") from None

    def get_data_request(self, endpoint, headers=None, params=None):
        headers = headers or {}
        headers["Authorization"] = f"Bearer {self.token}"
        headers["APIGW-Tracking-Header"] = str(uuid.uuid4())

        all_data = []  # List to store the complete data from all pages
        current_page = 1

        while True:
            try:
                params["page"] = current_page
                response = requests.get(  # noqa: S113
                    endpoint, headers=headers, params=params
                )
                response.raise_for_status()

                data = response.json().get("data")
                all_data.extend(data)

                pagination_metadata = response.json().get("pagination_metadata")
                next_page_number = pagination_metadata.get("next_page_number")

                if next_page_number is not None:
                    current_page = next_page_number
                else:
                    break

            except requests.exceptions.RequestException as e:
                logger.error(f"Request exception occurred: {str(e)}")
                raise APIClientException("Failed to make the request.") from None
            except Exception as e:
                logger.error(f"An unexpected error occurred: {str(e)}")
                raise APIClientException("An unexpected error occurred.") from None

        return all_data
