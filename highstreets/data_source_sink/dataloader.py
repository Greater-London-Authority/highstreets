import logging

from highstreets.api.clientbase import APIClient, APIClientException


class DataLoaderException(Exception):
    pass


class DataLoader:
    def __init__(self, api_endpoint):
        self.api_endpoint = api_endpoint
        self.api_client = APIClient()
        self.logger = logging.getLogger(__name__)

    def get_hex_data(self, date_from, date_to):
        params = {"date_from": date_from, "date_to": date_to}

        try:
            return self.api_client.get_data_request(self.api_endpoint, params=params)
        except APIClientException as e:
            self.logger.error(str(e))
            raise DataLoaderException("Failed to fetch data.") from None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")
            raise DataLoaderException("An unexpected error occurred.") from None
