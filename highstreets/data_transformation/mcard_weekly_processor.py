# file_processor.py
import os
import re
import pandas as pd
import geopandas as gpd
from datetime import datetime
from glob import glob
import logging


class FileProcessor:
    def __init__(self, data_loader, data_writer, dir_path: str):
        self.data_loader = data_loader
        self.data_writer = data_writer
        self.dir_path = dir_path
        self.new_files = []
        self.existing_files = []
        logging.info("FileProcessor initialized.")

    @staticmethod
    def extract_date_from_filename(filename: str):
        date_match = re.search(r'_(\d{8})_(\d{8})_', filename)
        if date_match:
            start_date_str = date_match.group(1)
            end_date_str = date_match.group(2)
            start_date = datetime.strptime(start_date_str, '%Y%m%d')
            end_date = datetime.strptime(end_date_str, '%Y%m%d')
            return start_date, end_date
        return None, None

    def process_borough_hs_lookup(self, data_loader):
        context_areas = data_loader.get_query_context(layers=[
            "BIDs", "Highstreets", "TownCentres", "Bespoke"])
        boroughs = data_loader.get_query_context(layers=["Boroughs"])
        # Process context areas
        context_areas['name'] = context_areas['name'].str.replace("â", "'")

        # Conditional assignment of names and IDs
        context_areas = context_areas.assign(
            highstreet_name=context_areas.apply(
                lambda x: x['name'] if x['layer'] == "Highstreets" else pd.NA, axis=1),
            bid_name=context_areas.apply(
                lambda x: x['name'] if x['layer'] == "BIDs" else pd.NA, axis=1),
            tc_name=context_areas.apply(
                lambda x: x['name'] if x['layer'] == "TownCentres" else pd.NA, axis=1),
            bespoke_name=context_areas.apply(
                lambda x: x['name'] if x['layer'] == "Bespoke" else pd.NA, axis=1),
            highstreet_id=context_areas.apply(
                lambda x: int(x['id']) if x[
                    'layer'] == "Highstreets" else pd.NA, axis=1),
            bid_id=context_areas.apply(
                lambda x: int(x['id']) if x['layer'] == "BIDs" else pd.NA, axis=1),
            tc_id=context_areas.apply(
                lambda x: int(x['id']) if x[
                    'layer'] == "TownCentres" else pd.NA, axis=1),
            bespoke_id=context_areas.apply(
                lambda x: int(x['id']) if x['layer'] == "Bespoke" else pd.NA, axis=1),
        )

        # Select and join with boroughs
        context_areas = context_areas.drop(columns=["name", "id", "layer"])

        # Apply a negative buffer to boroughs (to avoid slithers)
        boroughs['geometry'] = boroughs['geometry'].buffer(-29)

        # Spatial join with boroughs
        df = gpd.sjoin(context_areas, boroughs, how="inner", op="intersects")
        # drop geometry column
        df = df.drop(columns=["geometry"]).reset_index(drop=True)
        # rename columns
        df.rename(columns={"name": "borough_name", "id": "borough_code"}, inplace=True)

        df = df.sort_values(['bid_id', 'highstreet_id', 'tc_id', 'bespoke_id'])

        # Add objectid and select final columns
        df['objectid'] = range(1, len(df) + 1)
        df = df[['objectid', 'borough_name', 'borough_code', 'highstreet_name',
                 'highstreet_id', 'bid_name', 'bid_id', 'tc_name',
                'tc_id', 'bespoke_name', 'bespoke_id']]
        return df

    @staticmethod
    def is_newer_than_recent(file_start_date, recent_year, recent_week) -> bool:
        file_year, file_week = file_start_date.isocalendar()[:2]
        return (file_year > recent_year) or (
            file_year == recent_year and file_week > recent_week)

    def process_mcard_raw_files(self, table_name_map: dict):
        recent_dates = self.data_loader.get_most_recent_dates(table_name_map.values())
        files = glob(os.path.join(self.dir_path, "*.csv"))

        for file in files:
            zoom_level_match = re.search(r'(\d+)_zoom', file.lower())
            if zoom_level_match:
                zoom_level = zoom_level_match.group(1)
                table_name = table_name_map.get(zoom_level)
                if not table_name:
                    logging.warning(f"No table name found for zoom level {zoom_level}."
                                    f" Skipping file {file}.")
                    continue

                file_start_date, _ = self.extract_date_from_filename(file)

                if table_name in recent_dates:
                    recent_year, recent_week = recent_dates[table_name]
                    if not self.is_newer_than_recent(file_start_date,
                                                     recent_year, recent_week):
                        self.existing_files.append(os.path.basename(file))
                        logging.info(f"File {file} already present in the database."
                                     f" Skipping...")
                        continue

                self._process_file(file, table_name)

        self._log_results()

    def clean_and_process_data(self, zoom, cols, clean_table_name, segment="Overall",
                               geo_name="London"):
        # Reuse the get_most_recent_dates function to check for existing data
        recent_dates = self.data_loader.get_most_recent_dates([clean_table_name])

        if clean_table_name in recent_dates:
            last_yr, last_wk = recent_dates[clean_table_name]
            logging.info(f"Existing data found in {clean_table_name}."
                         f" Processing raw data from year {last_yr}, week {last_wk}.")
        else:
            last_yr, last_wk = None, None
            logging.info(f"No existing data in {clean_table_name}."
                         f" Processing all raw data.")

        # Retrieve raw data
        df_raw = self.data_loader.query_mcard_raw_since(
            zoom,
            cols,
            last_yr,
            last_wk,
            segment,
            geo_name) if last_yr else self.data_loader.query_mcard_weekly_raw(zoom, cols)

        if df_raw.empty:
            logging.info(f"Most recent data already exists in {clean_table_name}."
                         f" No new data to process.")
            return

        # Clean the data
        dates = df_raw[["yr", "wk", "weekday_weekend"]].drop_duplicates()
        dates['yr'] = dates['yr'].astype('Int64')
        dates['wk'] = dates['wk'].astype('Int64')
        dates['week_start'] = dates.apply(lambda row: self.data_loader.get_week_start(
            row['yr'], row['wk']), axis=1)

        locs = df_raw[
            ["quad_id", "central_latitude", "central_longitude"]].drop_duplicates()

        files = df_raw["file_name"].unique()
        if any("12Apr2021_18Apr2021" in file for file in files):
            df_raw = df_raw[~((df_raw["yr"] == 2021) & (df_raw["wk"] == 15) & df_raw[
                "file_name"].str.contains("05Apr2021_02May2021"))]
        df_raw = df_raw[~((df_raw["yr"] == 2020) & (df_raw["wk"] == 49) & df_raw[
            "file_name"].str.contains("Nov"))]

        df_clean = df_raw.drop(
            columns=["file_name", "central_latitude", "central_longitude"])
        df_clean['yr'] = df_clean['yr'].astype('Int64')
        df_clean['wk'] = df_clean['wk'].astype('Int64')
        df_clean = df_clean.merge(dates, on=["yr", "wk", "weekday_weekend"], how="inner")
        df_clean = df_clean.merge(locs, on="quad_id", how="left")

        logging.info("Data cleaned successfully.")
        # Write the cleaned data to the database
        self.data_writer.append_chunk(df_clean, clean_table_name)
        logging.info(f"Cleaned data appended to table {clean_table_name}.")

    def _process_file(self, file: str, table_name: str):
        day_end = "weekend" if "weekend" in file.lower() else "weekday"
        chunk_size = 200000
        # table_name = f"test_econ_busyness_mcard_raw_{zoom_level}_zoom"

        reader = pd.read_csv(file, chunksize=chunk_size, delimiter='|', dtype={
            'yr': float, 'wk': float, 'industry': str, 'segment': str, 'geo_type': str,
            'geo_name': str, 'quad_id': str, 'central_latitude': float,
            'central_longitude': float, 'bounding_box': str, 'txn_amt': float,
            'txn_cnt': float, 'acct_cnt': float,
            'avg_ticket': float, 'avg_freq': float, 'avg_spend_amt': float,
            'yoy_txn_amt': str, 'yoy_txn_cnt': str
        })

        for i, chunk in enumerate(reader):
            chunk['weekday_weekend'] = day_end
            chunk['file_name'] = os.path.basename(file)
            self.data_writer.append_chunk(chunk, table_name)

        self.new_files.append(os.path.basename(file))
        logging.info(f"Processed and uploaded file {file}.")

    def _log_results(self):
        logging.info("New files uploaded to the database:")
        for new_file in self.new_files:
            logging.info(f" - {new_file}")

        logging.info("Files already present in the database:")
        for existing_file in self.existing_files:
            logging.info(f" - {existing_file}")
