import logging
import uuid

import requests

# from highstreets.api.client_base import APIClient


class DataLoader:
    def __init__(self, api_endpoint, token):
        self.api_endpoint = api_endpoint
        self.token = token
        self.logger = logging.getLogger(__name__)

    def get_hex_data(self, date_from, date_to):
        params = {"date_from": date_from, "date_to": date_to}
        all_data = []  # List to store the complete data from all pages
        current_page = 1

        while True:
            try:
                params["page"] = current_page
                headers = {
                    "Authorization": f"Bearer {self.token}",
                    "APIGW-Tracking-Header": str(uuid.uuid4()),
                }
                response = requests.get(
                    self.api_endpoint, headers=headers, params=params, timeout=5
                )
                response.raise_for_status()  # Raise exception for unsuccessful requests

                data = response.json().get("data")
                all_data.extend(data)

                pagination_metadata = response.json().get("pagination_metadata")
                next_page_number = pagination_metadata.get("next_page_number")

                if next_page_number is not None:
                    current_page = next_page_number
                else:
                    break

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request exception occurred: {str(e)}")
                break

            except Exception as e:
                self.logger.error(f"An unexpected error occurred: {str(e)}")
                break

        return all_data
