# file_processor.py
import os
import re
import pandas as pd
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
