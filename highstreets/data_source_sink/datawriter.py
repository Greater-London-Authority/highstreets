import logging
import os

import geopandas as gpd
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from glapy.database.utils import fast_write
from sqlalchemy import create_engine, text

load_dotenv(find_dotenv())


class DataWriter:
    def __init__(self):
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
        self.hs_file_path = "/mnt/q/Projects/2019-20/Covid-19 Busyness/data/BT/test/"

    def load_data_to_csv(self, data, file_path):
        try:
            data.to_csv(file_path, index=False)
            logging.info(f"Data successfully loaded to CSV: {file_path}")
        except Exception as e:
            logging.error(f"Error while loading data to CSV: {e}")

    def table_exists(self, table_name):
        return self.engine.dialect.has_table(self.engine.connect(), table_name)

    def append_data_to_postgres(self, data, table_name):
        # Check if the table exists in the database
        if self.table_exists(table_name):
            try:
                # Get the max date in the table
                max_date = pd.read_sql_query(
                    text(f"SELECT MAX(count_date) FROM {table_name}"),  # noqa: S608
                    self.engine.connect(),
                )["max"][0]

                max_date = pd.to_datetime(max_date)
                # Convert date column to Date object
                data["count_date"] = pd.to_datetime(data["count_date"])

                # Filter the DataFrame to include only rows after the max date
                df_to_append = data[data["count_date"] > max_date]

                # Check if there are rows to append
                if len(df_to_append) > 0:
                    # Write the filtered data to the existing table
                    df_to_append.to_sql(
                        name=table_name,
                        con=self.engine,
                        if_exists="append",
                        index=False,
                        schema="gisapdata",
                    )
                    print("Data appended successfully.")
                    logging.info("Data successfully loaded to PostgreSQL")
                else:
                    print("No new data to append.")
            except Exception as e:
                print("Error occurred while appending data:", str(e))
                logging.error(f"Error while loading data to PostgreSQL: {e}")
        else:
            logging.info("The table does not exist")

        # Disconnect from the database
        try:
            self.engine.dispose()
            print("Disconnected from the database.")
        except Exception as e:
            print("Error occurred while disconnecting from the database:", str(e))

    def append_data_without_check(self, data, table_name):
        # Check if there are rows to append
        if len(data) > 0:
            # Write the filtered data to the existing table
            data.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="append",
                index=False,
                schema="gisapdata",
            )
            print("New Data appended successfully.")
            logging.info("Data successfully loaded to PostgreSQL")
        else:
            logging.info("No new data to append")

    def append_data_with_id_check(self, data, ids, id_col, table_name):
        if self.table_exists(table_name):
            try:
                if ids.size:
                    existing_ids_query = text(
                        f"SELECT DISTINCT {id_col} FROM {table_name} "  # noqa: S608
                        f"WHERE {id_col} IN ({', '.join(map(str, ids))})"
                    )
                    existing_ids = (
                        self.engine.connect().execute(existing_ids_query).fetchall()
                    )
                    if existing_ids:
                        for row in existing_ids:
                            logging.error("IDs already exist in the table: ", row[0])
                            print("Error: IDs already exist in the table.")
                        return False
                    else:
                        logging.info("Appending data to postgres")
                        self.append_data_without_check(data, table_name)
                        return True
            except Exception as e:
                error_msg = f"Error occurred while checking IDs: {str(e)}"
                logging.error(error_msg)

        else:
            logging.info("The table does not exist")

    def load_hsds_lookup_to_postgres(self):
        # bid
        query = (
            "select bid_id, bid_name, geom from "
            "regen_business_improvement_districts_27700_live"
        )
        bid = gpd.GeoDataFrame.from_postgis(
            text(query), self.engine.connect(), geom_col="geom"
        )

        # highstreet
        query = (
            "select highstreet_id, highstreet_name, geom "
            "from regen_high_streets_proposed_2"
        )
        highstreet = gpd.GeoDataFrame.from_postgis(
            text(query), self.engine.connect(), geom_col="geom"
        )

        # towncentre
        query = "select tc_id, tc_name, geom from planning_town_centre_all_2020"
        tc = gpd.GeoDataFrame.from_postgis(
            text(query), self.engine.connect(), geom_col="geom"
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
        frames = [highstreet, tc, bid]
        merged_df = pd.concat(frames)
        merged_df.info()
        merged_df = merged_df.replace("\n", "", regex=True)

        fast_write(merged_df, "hsds_bid_hs_tc", if_exists="truncate")

    def write_hex_to_csv_by_year(self, data, output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        data["count_date"] = pd.to_datetime(data["count_date"])

        for year, group in data.groupby(data["count_date"].dt.year):
            file_name = os.path.join(output_dir, f"hex_3hourly_counts_{year}.csv")
            group.to_csv(file_name, index=False)
            logging.info(f"Saved {file_name}")

    def write_threehourly_hs_to_csv(self, data):
        try:
            start_date = data["count_date"].min().strftime("%Y-%m-%d")
            end_date = data["count_date"].max().strftime("%Y-%m-%d")
            # Create the filename
            # name the file based on the id column name of data
            if data.columns[0] == "highstreet_id":
                filename = f"highstreets_3hourly_counts_{start_date}_{end_date}.csv"
            elif data.columns[0] == "tc_id":
                filename = f"towncentres_3hourly_counts_{start_date}_{end_date}.csv"
            elif data.columns[0] == "bespoke_area_id":
                filename = f"bespokes_3hourly_counts_{start_date}_{end_date}.csv"
            elif data.columns[0] == "bid_id":
                filename = f"bids_3hourly_counts_{start_date}_{end_date}.csv"
            # Write dataframe to CSV with the custom filename
            file_path = self.hs_file_path + filename
            pd.DataFrame(data).to_csv(file_path, index=False)
            logging.info(f"Data successfully written to CSV: {file_path}")
        except Exception as e:
            logging.error(f"Error while writing data to CSV: {e}")
