import logging
import os
import re
import fsspec
from datetime import datetime, timedelta
import requests
import psycopg2
from shapely.geometry import shape

import geopandas as gpd
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Float

from highstreets import config
from highstreets.api.clientbase import APIClient, APIClientException

load_dotenv(find_dotenv())


class DataLoaderException(Exception):
    pass


class SchemaMismatchError(Exception):
    pass


class DateRangeError(Exception):
    pass


class DataLoader:
    def __init__(self):
        self.hex_api_endpoint = config.BT_HEX_API_ENDPOINT
        self.msoa_api_endpoint = config.BT_MSOA_API_ENDPOINT
        self.lsoa_api_endpoint = config.BT_LSOA_API_ENDPOINT
        self.bt_hourly_outage_api_endpoint = config.BT_HOURLY_OUTAGE_API_ENDPOINT
        self.bt_outage_history_api_endpoint = config.BT_OUTAGE_HISTORY_API_ENDPOINT
        self.bt_daily_aggregated_shapes = config.BT_DAILY_AGGREGATED_SHAPES
        self.bt_catchment_visitor_api = config.BT_CATCHMENT_VISITOR_API_ENDPOINT
        self.bt_catchment_worker_api = config.BT_CATCHMENT_WORKER_API_ENDPOINT
        self.base_dir = config.BASE_DIR
        self.api_client = APIClient()
        self.logger = logging.getLogger(__name__)
        self.database = os.getenv("PG_DATABASE")
        self.username = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASSWORD")
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT")
        # Create a database connection
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
        self.conn = psycopg2.connect(
            dbname=self.database,
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)
        logging.info("Database engine created and metadata reflected.")

    def create_table_mcard_weekly_raw(self, table_name: str):
        Table(
            table_name, self.metadata,
            Column('yr', Float),
            Column('wk', Float),
            Column('industry', String),
            Column('segment', String),
            Column('geo_type', String),
            Column('geo_name', String),
            Column('quad_id', String),
            Column('central_latitude', Float),
            Column('central_longitude', Float),
            Column('bounding_box', String),
            Column('txn_amt', Float),
            Column('txn_cnt', Float),
            Column('acct_cnt', Float),
            Column('avg_ticket', Float),
            Column('avg_freq', Float),
            Column('avg_spend_amt', Float),
            Column('yoy_txn_amt', String),
            Column('yoy_txn_cnt', String),
            Column('weekday_weekend', String),  # New column
            Column('file_name', String),         # New column
            extend_existing=True    # Allow redefinition of existing table
        )
        self.metadata.create_all(self.engine)
        logging.info(f"Ensured table {table_name} exists or created it.")

    def get_query_context(self, layers=None):
        if layers is None:
            layers = ["BIDs",
                      "CAZ",
                      "Highstreets",
                      "TownCentres",
                      "Boroughs",
                      "MSOAs",
                      "Bespoke"]

        layer_nums = {
            "BIDs": 1,
            "CAZ": 2,
            "Highstreets": 3,
            "TownCentres": 5,
            "Boroughs": 0,
            "MSOAs": 4,
            "Bespoke": 8
        }

        layer_ids = {
            "BIDs": ["bid_id", "bid_name"],
            "CAZ": ["objectid", "name"],
            "Highstreets": ["highstreet_id", "highstreet_name"],
            "TownCentres": ["tc_id", "tc_name"],
            "Boroughs": ["gss_code", "name"],
            "MSOAs": ["msoa11cd", "msoa11nm"],
            "Bespoke": ["bespoke_area_id", "name"]
        }

        layer_df = []

        for layer in layers:
            layer_num = layer_nums[layer]
            layer_id = layer_ids[layer]

            service_query = (
                f"https://gis2.london.gov.uk/server/rest/services/apps"
                f"/Busyness_context/MapServer/"
                f"{layer_num}/query?where=1%3D1&outFields=*&f=geojson"
            )

            # Fetch the data
            response = requests.get(service_query)
            data = response.json()

            # Convert to GeoDataFrame
            features = data['features']
            geoms = [shape(feature['geometry']) for feature in features]
            records = [feature['properties'] for feature in features]
            df = gpd.GeoDataFrame(records, geometry=geoms, crs="EPSG:4326")

            # Specific adjustment for CAZ
            if layer == "CAZ":
                df['name'] = "CAZ"

            # Select and rename the columns
            df = df[layer_id + ['geometry']].rename(columns={
                layer_id[0]: 'id',
                layer_id[1]: 'name'
            })
            # Transform to British National Grid (EPSG:27700)
            df = df.to_crs(epsg=27700)

            # Add layer information
            df['layer'] = layer
            df['id'] = df['id'].astype(str)
            df['name'] = df['name'].str.replace("â€™", "'")
            layer_df.append(df)

        # Combine all layers into a single DataFrame
        df = pd.concat(layer_df, ignore_index=True)
        return df

    def get_most_recent_dates(self, table_names: list) -> dict:
        recent_dates = {}
        for table_name in table_names:
            if table_name in self.metadata.tables:
                with self.engine.connect() as connection:
                    max_year_query = text(f"SELECT MAX(yr) as max_yr FROM {table_name}")
                    max_year_result = connection.execute(max_year_query).fetchone()
                    if max_year_result and max_year_result[0] is not None:
                        max_year = max_year_result[0]
                        max_week_query = text(
                            f"SELECT MAX(wk) as max_wk FROM {table_name}"
                            f" WHERE yr = :max_year")
                        max_week_result = connection.execute(
                            max_week_query, {'max_year': max_year}).fetchone()
                        if max_week_result and max_week_result[0] is not None:
                            max_week = max_week_result[0]
                            recent_dates[table_name] = (max_year, max_week)
                            logging.info(
                                f"Most recent date for table"
                                f" {table_name}: {max_year}-W{max_week}")
                        else:
                            logging.info(
                                f"No max week found for table"
                                f" {table_name} with year {max_year}.")
                    else:
                        logging.info(f"No max year found for table {table_name}.")
        return recent_dates

    @staticmethod
    def get_week_start(year, week):
        jan4 = datetime(year, 1, 4)
        start_of_first_week = jan4 - timedelta(days=jan4.weekday())
        start_date = start_of_first_week + timedelta(weeks=week - 1)
        return start_date.strftime('%Y-%m-%d')

    def query_from_file(self, filepath):
        """Read, execute and load execute SQL query from a file"""
        with open(filepath, 'r') as file:
            query = file.read()
        return pd.read_sql_query(query, self.conn)

    def close_connection(self):
        """close the PostgreSQL connection"""
        self.conn.close()

    def query_mcard_raw_since(self, zoom, cols, last_yr, last_wk, segment="Overall",
                              geo_name="London"):
        table_name = f"econ_busyness_mcard_raw_{zoom}_zoom"
        columns = ", ".join(cols) if cols else "*"
        query = text(f"""
            SELECT {columns}
            FROM {table_name}
            WHERE ((yr > {last_yr}) OR (yr = {last_yr} AND wk > {last_wk}))
            AND segment = :segment AND geo_name = :geo_name
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql_query(
                query, conn, params={'segment': segment, 'geo_name': geo_name})
        logging.info(
            f"Queried raw data from {table_name} starting from year"
            f" {last_yr}, week {last_wk}.")
        return df

    def query_mcard_weekly_raw(self, zoom=18,
                               cols=None,
                               quad_id=None,
                               industry=None,
                               segment="Overall",
                               geo_name="London",
                               weekday_weekend=None,
                               yr=None, wk=None, v="v1"):
        table_name = f"econ_busyness_mcard_raw_{zoom}_zoom"
        if v == "v2":
            table_name += "_v2"

        query_args = {
            "quad_id": quad_id,
            "industry": industry,
            "segment": segment,
            "geo_name": geo_name,
            "weekday_weekend": weekday_weekend,
            "yr": yr,
            "wk": wk
        }

        filters = [
            f"{k}='{v}'" if isinstance(v, str)
            else f"{k} IN ({','.join(map(str, v))})"
            for k, v in query_args.items()
            if v is not None
        ]
        filter_clause = " AND ".join(filters)

        columns = ", ".join(cols) if cols else "*"
        query = f"SELECT {columns} FROM {table_name}" + (
            f" WHERE {filter_clause}" if filter_clause else "")

        with self.engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn)
        return df

    def get_hex_data(self, date_from, date_to):
        params = {"date_from": date_from, "date_to": date_to}

        try:
            return self.api_client.get_data_request(
                self.hex_api_endpoint, params=params
            )
        except APIClientException as e:
            self.logger.error(str(e))
            raise DataLoaderException("Failed to fetch data.") from None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")
            raise DataLoaderException("An unexpected error occurred.") from None

    def get_catchment_visitor_data(self, date_from, date_to):
        params = {"date_from": date_from, "date_to": date_to}

        try:
            return self.api_client.get_data_request(
                self.bt_catchment_visitor_api, params=params
            )
        except APIClientException as e:
            self.logger.error(str(e))
            raise DataLoaderException("Failed to fetch data.") from None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")
            raise DataLoaderException("An unexpected error occurred.") from None

    def get_catchment_visitor_data_by_poi(
        self,
        poi_id: str,
        poi_type: str,
        date_from: str,
        date_to: str,
        month_by_month: bool = True,
    ) -> pd.DataFrame:
        """Fetch catchment visitor data filtered by poi_id and poi_type.

        Calls the same endpoint as get_catchment_visitor_data but includes
        poi_id and poi_type in the request so the API filters server-side.

        Args:
            poi_id: e.g. "TOWN00138", "64"
            poi_type: e.g. "towncentres", "bids", "highstreets"
            date_from: start date (first-of-month, YYYY-MM-DD)
            date_to: end date (first-of-month, YYYY-MM-DD)
            month_by_month: iterate one month at a time to keep responses
                            manageable. Set False for a single request.
        """
        endpoint = self.bt_catchment_visitor_api

        if not month_by_month:
            params = {
                "poi_id": poi_id,
                "poi_type": poi_type,
                "date_from": date_from,
                "date_to": date_to,
            }
            self.logger.info(
                f"Fetching {date_from} -> {date_to} for {poi_type}/{poi_id}"
            )
            data = self.api_client.get_data_request(endpoint, params=params)
            return pd.DataFrame(data)

        all_frames = []
        current = pd.Timestamp(date_from)
        end = pd.Timestamp(date_to)

        while current <= end:
            month_str = current.strftime("%Y-%m-%d")
            params = {
                "poi_id": poi_id,
                "poi_type": poi_type,
                "date_from": month_str,
                "date_to": month_str,
            }
            self.logger.info(f"Fetching {month_str} for {poi_type}/{poi_id} ...")
            try:
                data = self.api_client.get_data_request(endpoint, params=params)
                if data:
                    all_frames.append(pd.DataFrame(data))
                    self.logger.info(f"  -> {len(data)} rows")
                else:
                    self.logger.warning(f"  -> no data for {month_str}")
            except Exception as e:
                self.logger.error(f"  -> failed for {month_str}: {e}")

            current += pd.DateOffset(months=1)

        if not all_frames:
            self.logger.warning("No data returned for any month.")
            return pd.DataFrame()

        return pd.concat(all_frames, ignore_index=True)

    def get_catchment_worker_data(self, date_from, date_to):
        params = {"date_from": date_from, "date_to": date_to}

        try:
            return self.api_client.get_data_request(
                self.bt_catchment_worker_api, params=params
            )
        except APIClientException as e:
            self.logger.error(str(e))
            raise DataLoaderException("Failed to fetch data.") from None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")
            raise DataLoaderException("An unexpected error occurred.") from None

    def get_catchment_worker_data_by_poi(
        self,
        poi_id: str,
        poi_type: str,
        date_from: str,
        date_to: str,
        month_by_month: bool = True,
    ) -> pd.DataFrame:
        """Fetch catchment worker data filtered by poi_id and poi_type.

        Same as get_catchment_worker_data but with server-side POI filtering.
        """
        endpoint = self.bt_catchment_worker_api

        if not month_by_month:
            params = {
                "poi_id": poi_id,
                "poi_type": poi_type,
                "date_from": date_from,
                "date_to": date_to,
            }
            data = self.api_client.get_data_request(endpoint, params=params)
            return pd.DataFrame(data)

        all_frames = []
        current = pd.Timestamp(date_from)
        end = pd.Timestamp(date_to)

        while current <= end:
            month_str = current.strftime("%Y-%m-%d")
            params = {
                "poi_id": poi_id,
                "poi_type": poi_type,
                "date_from": month_str,
                "date_to": month_str,
            }
            self.logger.info(
                f"Fetching worker {month_str} for {poi_type}/{poi_id} ..."
            )
            try:
                data = self.api_client.get_data_request(endpoint, params=params)
                if data:
                    all_frames.append(pd.DataFrame(data))
            except Exception as e:
                self.logger.error(f"  -> failed for {month_str}: {e}")

            current += pd.DateOffset(months=1)

        if not all_frames:
            return pd.DataFrame()

        return pd.concat(all_frames, ignore_index=True)

    def get_msoa_data(self, date_from, date_to):
        params = {"date_from": date_from, "date_to": date_to}

        try:
            return self.api_client.get_data_request(
                self.msoa_api_endpoint, params=params
            )  # noqa: E501
        except APIClientException as e:
            self.logger.error(str(e))
            raise DataLoaderException("Failed to fetch data.") from None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")
            raise DataLoaderException("An unexpected error occurred.") from None

    def get_lsoa_data(self, date_from, date_to):
        params = {"date_from": date_from, "date_to": date_to}

        try:
            return self.api_client.get_data_request(
                self.lsoa_api_endpoint, params=params
            )  # noqa: E501
        except APIClientException as e:
            self.logger.error(str(e))
            raise DataLoaderException("Failed to fetch data.") from None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")
            raise DataLoaderException("An unexpected error occurred.") from None

    def get_bt_hourly_outage_data(self, date_from, date_to):
        params = {"date_from": date_from, "date_to": date_to}

        try:
            return self.api_client.get_data_request(
                self.bt_hourly_outage_api_endpoint, params=params
            )  # noqa: E501
        except APIClientException as e:
            self.logger.error(str(e))
            raise DataLoaderException("Failed to fetch data.") from None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")
            raise DataLoaderException("An unexpected error occurred.") from None

    def get_bt_outage_history_data(self, date_from, date_to):
        params = {"date_from": date_from, "date_to": date_to}

        try:
            return self.api_client.get_data_request(
                self.bt_outage_history_api_endpoint, params=params
            )  # noqa: E501
        except APIClientException as e:
            self.logger.error(str(e))
            raise DataLoaderException("Failed to fetch data.") from None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")
            raise DataLoaderException("An unexpected error occurred.") from None

    def get_partial_data(self, table_name, columns, where_clause):
        """
        Retrieve partial data from a PostgreSQL table into a DataFrame.

        Parameters:
            table_name (str): The name of the table in the PostgreSQL database.
            columns (list): Columns to load from table.
            where_clause (str): SQL where clause of subset of data to load.

        Returns:
            pandas.DataFrame: The DataFrame containing required data from the table.
        """
        logging.info("Getting partial data from mc weekly zoom 18 in PG")
        try:
            # Establish a connection to the PostgreSQL database using SQLAlchemy engine.
            with self.engine.connect() as connection:
                # Query to retrieve all data from the specified table.
                query = f"SELECT {columns} FROM {table_name} WHERE {where_clause}"  # noqa: S608 E501

                # Execute the query and fetch the data into a Pandas DataFrame.
                data_df = pd.read_sql(text(query), connection)

                # Return the DataFrame containing the full data.
                return data_df
        except Exception as e:
            # Handle any potential errors gracefully.
            print(f"An error occurred while fetching data: {e}")

        return None

    def get_bt_daily_aggregate_customer_shapes(self, date_from, date_to):
        params = {"date_from": date_from, "date_to": date_to}

        try:
            return self.api_client.get_data_request(
                self.bt_daily_aggregated_shapes, params=params
            )  # noqa: E501
        except APIClientException as e:
            self.logger.error(str(e))
            raise DataLoaderException("Failed to fetch data.") from None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")
            raise DataLoaderException("An unexpected error occurred.") from None

    def get_full_data(self, table_name):
        """
        Retrieve full data from a PostgreSQL table into a DataFrame.

        Parameters:
            table_name (str): The name of the table in the PostgreSQL database.

        Returns:
            pandas.DataFrame: The DataFrame containing the full data from the table.
        """
        try:
            # Establish a connection to the PostgreSQL database using SQLAlchemy engine.
            with self.engine.connect() as connection:
                # Query to retrieve all data from the specified table.
                query = f"SELECT * FROM {table_name}"  # noqa: S608

                # Execute the query and fetch the data into a Pandas DataFrame.
                data_df = pd.read_sql(text(query), connection)

                # Return the DataFrame containing the full data.
                return data_df

        except Exception as e:
            # Handle any potential errors gracefully.
            print(f"An error occurred while fetching data: {e}")
            return None

    def get_hex_lookup(self, lookup_type):
        # Load the .shp file using GeoPandas
        hex350_grid_GLA = gpd.read_file(
            f"{self.base_dir}"
            "reference_data/shapefiles/hex350_grid_GLA.shp"
        )
        hex_400m_buffer1 = gpd.read_file(
            f"{self.base_dir}"
            "reference_data/shapefiles/hex_400m_buffer1.shp"
        )
        hex_400m_buffer1 = hex_400m_buffer1.rename(columns={"Hex_ID": "hex_id"})

        if lookup_type == "highstreet":
            query = (
                "select highstreet_id, highstreet_name, geom "
                "from regen_high_streets_proposed_2"
            )
            highstreet = gpd.GeoDataFrame.from_postgis(
                text(query), self.engine.connect(), geom_col="geom"
            )
            # Spatially join the two GeoDataFrames based on the geometry intersection
            join_result = gpd.sjoin(
                hex_400m_buffer1, highstreet, how="left", op="intersects"
            )
            # Select the 'Hex_ID', 'highstreet_id', and 'geometry' columns
            lookup_table = join_result[["hex_id", "highstreet_id", "highstreet_name"]]
            lookup_table["highstreet_id"] = lookup_table["highstreet_id"].astype(
                "Int64"
            )
            return lookup_table
        elif lookup_type == "towncentre":
            query = "select tc_id, tc_name, geom from planning_town_centre_all_2020"
            tc = gpd.GeoDataFrame.from_postgis(
                text(query), self.engine.connect(), geom_col="geom"
            )
            # Spatially join the two GeoDataFrames based on the geometry intersection
            join_result = gpd.sjoin(hex_400m_buffer1, tc, how="left", op="intersects")
            lookup_table = join_result[["hex_id", "tc_id", "tc_name"]]
            lookup_table["tc_id"] = lookup_table["tc_id"].astype("Int64")
            return lookup_table
        elif lookup_type == "bespoke":
            query = (
                "select bespoke_area_id, name, "
                "geometry from econ_busyness_bespoke_focus_areas_live"
            )
            bespoke = gpd.GeoDataFrame.from_postgis(
                text(query), self.engine.connect(), geom_col="geometry"
            )
            # Spatially join the two GeoDataFrames based on the geometry intersection
            join_result = gpd.sjoin(
                hex350_grid_GLA, bespoke, how="left", op="intersects"
            )
            lookup_table = join_result[["hex_id", "bespoke_area_id", "name"]]
            lookup_table["bespoke_area_id"] = lookup_table["bespoke_area_id"].astype(
                "Int64"
            )
            return lookup_table
        elif lookup_type == "bid":
            query = (
                "select bid_id, bid_name, geom "
                "from regen_business_improvement_districts_27700_live"
            )
            bid = gpd.GeoDataFrame.from_postgis(
                text(query), self.engine.connect(), geom_col="geom"
            )
            # Spatially join the two GeoDataFrames based on the geometry intersection
            join_result = gpd.sjoin(hex_400m_buffer1, bid, how="left", op="intersects")
            lookup_table = join_result[["hex_id", "bid_id", "bid_name"]]
            lookup_table["bid_id"] = lookup_table["bid_id"].astype("Int64")
            return lookup_table

    def mcard_3hourly_latest_data_read(self, mcard_source_path):
        """Read latest Mastercard 3-hourly transaction data from a directory.

        Scans a directory for CSV files containing Mastercard transaction data,
        identifies the most recent file based on date ranges in filenames, and loads
        that data. Files should have names containing date ranges in format
        YYYYMMDD_YYYYMMDD. The data is validated against an expected schema of
        transaction metrics.

        Parameters
        ----------
        mcard_source_path : str
            Path to directory containing CSV files. Can be local filesystem path or
            remote path (s3://, gs://, etc.) supported by fsspec.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing the latest Mastercard transaction data.

        Raises
        ------
        DateRangeError
            If no valid date ranges are found in the filenames.
        SchemaMismatchError
            If loaded data does not match expected schema.
        """
        """Read latest Mastercard 3-hourly data from any filesystem supported by fsspec

        Args:
            mcard_source_path: Path to directory containing CSV files
                             (local, s3://, gs://, etc.)
        """
        try:
            latest_date_range = None
            latest_filename = None

            # Get filesystem based on protocol in path
            fs = fsspec.filesystem(fsspec.utils.get_protocol(mcard_source_path))

            # List all files in directory
            files = fs.glob(f"{mcard_source_path}*.csv")

            for filepath in files:
                filename = os.path.basename(filepath)
                # Print filename for debugging
                self.logger.debug(f"Found file: {filename}")

                # Update regex to handle both date formats
                date_match = re.search(r"(\d{8})(?:_(\d{8})|$)", filename)
                if date_match:
                    start_date_str = date_match.group(1)
                    # If end date not in filename, use start date
                    end_date_str = (date_match.group(2) if date_match.group(2)
                                    else start_date_str)
                    start_date = datetime.strptime(start_date_str, "%Y%m%d")
                    end_date = datetime.strptime(end_date_str, "%Y%m%d")

                    # Log dates for debugging
                    self.logger.debug(f"File {filename}: {start_date} - {end_date}")

                    if not latest_date_range or end_date > latest_date_range[1]:
                        latest_date_range = (start_date, end_date)
                        latest_filename = filename
                        self.logger.debug(f"New latest file: {filename}")

            if not latest_date_range:
                raise DateRangeError("No valid date range found in filenames")

            self.logger.info(
                f"Latest date range: {latest_date_range[0]} - {latest_date_range[1]}"
                f" from {latest_filename}"
            )

            # Read CSV using fsspec
            with fs.open(os.path.join(mcard_source_path, latest_filename)) as f:
                df = pd.read_csv(f, sep="|")

            # Validate schema
            expected_columns = [
                "yr", "txn_date", "time_slot", "industry", "segment",
                "geo_type", "geo_name", "quad_id", "central_latitude",
                "central_longitude", "bounding_box", "txn_amt", "txn_cnt",
                "acct_cnt", "avg_ticket", "avg_freq", "avg_spend_amt",
                "yoy_txn_amt", "yoy_txn_cnt",
            ]

            if list(df.columns) != expected_columns:
                raise SchemaMismatchError("CSV schema does not match expected columns")

            # Convert and validate dates
            df["txn_date"] = pd.to_datetime(df["txn_date"])
            date_mask = (df["txn_date"] < latest_date_range[0]) | (
                df["txn_date"] > latest_date_range[1]
            )
            if date_mask.any():
                raise DateRangeError(
                    "CSV data contains dates outside the specified range")

            return df

        except SchemaMismatchError as sme:
            self.logger.error(f"Schema Mismatch: {sme}")
            raise sme

        except DateRangeError as dre:
            self.logger.error(f"Date Range Error: {dre}")
            raise dre

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            raise e
