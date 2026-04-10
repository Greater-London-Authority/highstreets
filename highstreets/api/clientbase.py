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
        Fetch CPI data using ONS API v4 (cpih01), with mm23 fallback for
        months that cpih01 hasn't published yet.

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

        logger.info(f"Fetching CPI data from ONS cpih01 API, version: {version}")

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

        latest_yr = cpi_table['yr'].max()
        latest_mo = cpi_table.loc[
            cpi_table['yr'] == latest_yr, 'month'
        ].max()
        logger.info(
            f"cpih01: fetched {len(cpi_table)} observations, "
            f"latest: {latest_yr}-{latest_mo:02d}"
        )

        # Step 4: mm23 fallback for months beyond cpih01's latest release
        supplement = self._fetch_cpi_mm23_supplement(cpi_table)
        if supplement is not None and len(supplement) > 0:
            cpi_table = pd.concat([cpi_table, supplement], ignore_index=True)

        return cpi_table

    def _fetch_cpi_mm23_supplement(self, cpih01_data):
        """
        Check the ONS mm23 time series for months newer than what cpih01 has.
        Returns rows only for complete months (all 13 categories present)
        that don't already exist in cpih01_data.

        Args:
            cpih01_data: DataFrame from cpih01 with columns
                [yr, month, Aggregate, cpi_index]

        Returns:
            pd.DataFrame | None: Supplementary rows, or None if nothing to add.
        """
        mm23_series = config.CPI_MM23_SERIES
        if not mm23_series:
            return None

        global_latest_yr = cpih01_data['yr'].max()
        global_latest_month = cpih01_data.loc[
            cpih01_data['yr'] == global_latest_yr, 'month'
        ].max()

        logger.info(
            f"mm23 fallback: cpih01 latest is "
            f"{global_latest_yr}-{global_latest_month:02d}, "
            f"checking mm23 for newer months..."
        )

        mm23_base_url = ("https://api.beta.ons.gov.uk/v1/data?uri="
                         "/economy/inflationandpriceindices/timeseries/{series_id}/mm23")

        all_mm23_rows = []
        failed_categories = []

        for category_label, series_id in mm23_series.items():
            url = mm23_base_url.format(series_id=series_id.lower())
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                months = resp.json().get('months', [])

                for m in months:
                    date_str = m.get('date', '')  # e.g. "2026 FEB"
                    value_str = m.get('value', '')
                    try:
                        dt = pd.to_datetime(date_str, format="%Y %b")
                        val = float(value_str)
                    except (ValueError, TypeError):
                        continue

                    yr, month = dt.year, dt.month
                    # Only keep months beyond cpih01's latest
                    is_newer = (
                        yr > global_latest_yr
                        or (yr == global_latest_yr
                            and month > global_latest_month)
                    )
                    if is_newer:
                        all_mm23_rows.append({
                            'yr': yr,
                            'month': month,
                            'Aggregate': category_label,
                            'cpi_index': val,
                        })

            except Exception as e:
                logger.warning(f"mm23 fallback: failed for {category_label} "
                               f"({series_id}): {e}")
                failed_categories.append(category_label)

        if not all_mm23_rows:
            logger.info("mm23 fallback: no newer months found beyond cpih01")
            return None

        supplement = pd.DataFrame(all_mm23_rows)

        # Only keep months where all 13 categories are present
        month_counts = supplement.groupby(['yr', 'month'])['Aggregate'].nunique()
        expected = len(mm23_series)
        complete_months = month_counts[month_counts == expected].index.tolist()

        if not complete_months:
            partial = month_counts[month_counts < expected]
            for (yr, mo), count in partial.items():
                logger.warning(f"mm23 fallback: {yr}-{mo:02d} only has "
                               f"{count}/{expected} categories -- skipped")
            return None

        supplement = supplement[
            supplement.set_index(['yr', 'month']).index.isin(complete_months)
        ].reset_index(drop=True)

        for yr, mo in complete_months:
            logger.info(
                f"mm23 fallback: supplementing {yr}-{mo:02d} "
                f"({expected} categories)"
            )

        return supplement

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
