import logging
import os

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text

from highstreets.data_source_sink.dataloader import DataLoader


class DataProcessor:
    def __init__(self):
        self.data_loader = DataLoader()
        load_dotenv(find_dotenv())
        self.database = os.getenv("PG_DATABASE")
        self.username = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASSWORD")
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT")
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
        self.hs_hex_ids = []
        self.tc_hex_ids = []
        self.bid_hex_ids = []
        self.bespoke_hex_ids = []

    def process_changes(self):
        query = """
            SELECT table_name, record_id, change_type, change_timestamp
            FROM econ_hsds_hs_bid_tc_bespoke_change_tracking
            WHERE processed = FALSE
            ORDER BY change_timestamp ASC
        """
        df = pd.read_sql_query(text(query), self.engine.connect())

        if not df.empty:
            for _index, row in df.iterrows():
                table_name, record_id, change_type, change_timestamp = row

                if change_type == "INSERT":
                    self.process_insert(table_name, record_id)

                elif change_type == "UPDATE":
                    self.process_update(table_name, record_id)

            # Update processed records
            self.update_processed_records(df["record_id"].tolist())
        else:
            logging.info("No new hs/bid/tc/bespoke updated or inserted")

    def process_insert(self, table_name, record_id):
        hex_ids = []

        if table_name == "highstreet":
            hex_ids = self.data_loader.get_hex_lookup("highstreet")
            self.hs_hex_ids.extend(
                [
                    (hex_id, record_id)
                    for hex_id in hex_ids.loc[
                        hex_ids["highstreet_id"] == record_id, "hex_id"
                    ].values
                ]
            )
        elif table_name == "towncentre":
            hex_ids = self.data_loader.get_hex_lookup("towncentre")
            self.tc_hex_ids.extend(
                [
                    (hex_id, record_id)
                    for hex_id in hex_ids.loc[
                        hex_ids["tc_id"] == record_id, "hex_id"
                    ].values
                ]
            )
        elif table_name == "bid":
            hex_ids = self.data_loader.get_hex_lookup("bid")
            self.bid_hex_ids.extend(
                [
                    (hex_id, record_id)
                    for hex_id in hex_ids.loc[
                        hex_ids["bid_id"] == record_id, "hex_id"
                    ].values
                ]
            )
        elif table_name == "bespoke":
            hex_ids = self.data_loader.get_hex_lookup("bespoke")
            self.bespoke_hex_ids.extend(
                [
                    (hex_id, record_id)
                    for hex_id in hex_ids.loc[
                        hex_ids["bespoke_area_id"] == record_id, "hex_id"
                    ].values
                ]
            )

        # Custom logic to process INSERT changes
        logging.info(f"INSERT: Change in table {table_name} for record ID {record_id}.")

    def process_update(self, table_name, record_id):
        # Custom logic to process UPDATE changes
        logging.info(f"UPDATE: Change in table {table_name} for record ID {record_id}.")

    def update_processed_records(self, processed_ids):
        if processed_ids:
            processed_query = f"""
                UPDATE econ_hsds_hs_bid_tc_bespoke_change_tracking
                SET processed = TRUE
                WHERE record_id IN ({', '.join(map(str, processed_ids))})
            """  # noqa: S608
            with self.engine.begin() as conn:
                conn.execute(text(processed_query))

            logging.info("Processed records updated.")

    def save_checkpoint_data(self):
        try:
            logging.info("Saving checkpoint data...")
            # Implement code to save checkpoint data to a separate table or file
            logging.info("Checkpoint data saved.")
        except Exception as e:
            logging.info(f"Error occurred while saving checkpoint data: {e}")
