import logging
import os

import geopandas as gpd
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text

from highstreets.api.clientbase import APIClient, APIClientException

load_dotenv(find_dotenv())


class DataLoaderException(Exception):
    pass


class DataLoader:
    def __init__(self):
        self.api_endpoint = (
            "https://api.business.bt.com/v1/footfall/reports/"
            "hex-grid/tfl?agg=time_indicator"
        )
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
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/"
            "data/reference_data/shapefiles/hex350_grid_GLA.shp"
        )
        hex_400m_buffer1 = gpd.read_file(
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/"
            "data/reference_data/shapefiles/hex_400m_buffer1.shp"
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
