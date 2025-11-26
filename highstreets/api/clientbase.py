import base64
import logging
import os
import uuid
import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv
from highstreets import config

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
        self.cpi_categories = config.CPI_CATEGORIES
        self.cpi_api = config.CPI_API_ENDPOINT

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

            # logger.info("Access token obtained successfully.")
            return token
        except requests.exceptions.RequestException as e:
            logger.error("Failed to obtain access token: %s", str(e))
            raise APIClientException("Failed to obtain access token.") from None

    def fetch_cpi(self):
        """
        Fetch CPI data using ONS API v4.

        Returns:
            pd.DataFrame: CPI data with columns ['yr', 'month', 'Aggregate', 'cpi_index']
        """
        dataset_id = "cpih01"
        edition = "time-series"

        # Get latest version
        response = requests.get(self.cpi_api)
        latest_version_url = response.json()['links']['latest_version']['href']
        latest_version = requests.get(latest_version_url)
        version = latest_version.json()['version']

        logger.info(f"Fetching CPI data from ONS API v4, version: {version}")

        # Step 1: Get all aggregate options to map labels to IDs
        aggregate_options_url = (
            f"https://api.beta.ons.gov.uk/v1/datasets/{dataset_id}/"
            f"editions/{edition}/versions/{version}/dimensions/aggregate/options"
        )
        agg_response = requests.get(aggregate_options_url, timeout=30)
        agg_items = agg_response.json().get('items', [])

        # Create mapping of label to ID
        agg_mapping = {item['label']: item['option'] for item in agg_items}

        # Step 2: Fetch observations for each required CPI category
        all_data = []
        observations_url = (
            f"https://api.beta.ons.gov.uk/v1/datasets/{dataset_id}/"
            f"editions/{edition}/versions/{version}/observations"
        )

        for category_label in self.cpi_categories:
            # Find matching aggregate ID
            agg_id = agg_mapping.get(category_label)

            if agg_id is None:
                logger.warning(f"CPI category '{category_label}' not found in API")
                continue

            # Fetch all time periods for this aggregate
            # (wildcard allowed for one dimension)
            params = {
                "time": "*",  # All time periods
                "geography": "K02000001",  # UK
                "aggregate": agg_id
            }

            obs_response = requests.get(observations_url, params=params, timeout=60)

            if obs_response.status_code == 200:
                observations = obs_response.json().get("observations", [])

                # Extract data from each observation
                for obs in observations:
                    time_id = obs['dimensions']['Time']['id']  # Format: "mmm-yy"
                    value = obs['observation']

                    # Convert string to float
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid observation value for"
                                       f" {category_label} at {time_id}: {value}")
                        continue

                    all_data.append({
                        'mmm-yy': time_id,
                        'Aggregate': category_label,
                        'v4_0': value
                    })
            else:
                logger.error(
                    f"Failed to fetch data for '{category_label}': "
                    f"Status {obs_response.status_code}"
                )

        # Step 3: Convert to DataFrame and process
        cpi_table = pd.DataFrame(all_data)

        # Parse dates and extract year/month
        cpi_table['date'] = pd.to_datetime(cpi_table["mmm-yy"], format="%b-%y")
        cpi_table['yr'] = cpi_table['date'].dt.year
        cpi_table['month'] = cpi_table['date'].dt.month

        # Filter, sort, and format to match original output structure
        cpi_table = (
            cpi_table[cpi_table['Aggregate'].isin(self.cpi_categories)]
            .sort_values('date')[['yr', 'month', 'Aggregate', 'v4_0']]
            .rename(columns={'v4_0': 'cpi_index'})
            .reset_index(drop=True)
        )

        logger.info(f"Successfully fetched {len(cpi_table)} CPI observations")

        return cpi_table

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

                if not data:  # check if data is empty
                    logger.error("No data available.")
                    break

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
