import logging
import os

import geopandas as gpd
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(find_dotenv())


class DataWriter:
    def __init__(self):
        self.database = os.getenv("PG_DATABASE")
        self.username = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASSWORD")
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT")

    def load_data_to_csv(self, data, file_path):
        try:
            data.to_csv(file_path, index=False)
            logging.info(f"Data successfully loaded to CSV: {file_path}")
        except Exception as e:
            logging.error(f"Error while loading data to CSV: {e}")

    def load_data_to_postgres(self, data, db_connection_string, table_name):
        try:
            # Code to load data to PostgreSQL goes here
            logging.info("Data successfully loaded to PostgreSQL")
        except Exception as e:
            logging.error(f"Error while loading data to PostgreSQL: {e}")

    def load_hsds_lookup_to_postgres(self):
        # Create a database connection
        engine = create_engine(
            f"postgresql+psycopg2://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
        # bid
        query = (
            "select bid_id, bid_name, geom from "
            "regen_business_improvement_districts_27700_backup"
        )
        bid = gpd.GeoDataFrame.from_postgis(
            text(query), engine.connect(), geom_col="geom"
        )

        # highstreet
        query = (
            "select highstreet_id, highstreet_name, geom "
            "from regen_high_streets_proposed_2"
        )
        highstreet = gpd.GeoDataFrame.from_postgis(
            text(query), engine.connect(), geom_col="geom"
        )

        # towncentre
        query = "select tc_id, tc_name, geom from planning_town_centre_all_2020"
        tc = gpd.GeoDataFrame.from_postgis(
            text(query), engine.connect(), geom_col="geom"
        )

        # Add a 'layer' column to each table to differentiate the data
        bid["layer"] = "bids"
        highstreet["layer"] = "highstreets"
        tc["layer"] = "towncentres"

        highstreet = highstreet.rename(
            columns={"highstreet_id": "id", "highstreet_name": "name"}
        )
        tc = tc.rename(columns={"tc_id": "id", "tc_name": "name"})
        bid = bid.rename(columns={"bid_id": "id", "bid_name": "name"})

        # Select the desired columns from each table
        bid = bid[["id", "name", "layer", "geom"]]
        highstreet = highstreet[["id", "name", "layer", "geom"]]
        tc = tc[["id", "name", "layer", "geom"]]

        # Merge the dataframes into a single dataframe
        merged_df = gpd.GeoDataFrame(
            pd.concat([highstreet, tc, bid], ignore_index=True)
        )

        merged_df.to_postgis(
            name="temp_hsds_bid_hs_tc",
            con=engine.connect(),
            if_exists="replace",
            index=False,
            schema="gisapdata",
        )

    def write_hex_to_csv(self, data):
        try:
            start_date = data["date"].min().strftime("%Y-%m-%d")
            end_date = data["date"].max().strftime("%Y-%m-%d")

            # Create the filename
            filename = f"hex_3hourly_counts_{start_date}_{end_date}.csv"

            # Write dataframe to CSV with the custom filename
            file_path = self.hex_file_path + filename
            data.to_csv(file_path, index=False)
            logging.info(f"Data successfully written to CSV: {file_path}")
        except Exception as e:
            logging.error(f"Error while writing data to CSV: {e}")
