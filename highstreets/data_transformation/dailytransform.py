import logging
import pandas as pd
import numpy as np

from highstreets.data_source_sink.dataloader import DataLoader


class DailyTransform(DataLoader):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler())

    def raw_bt_daily_preprocess_data(self, data):
        self.logger.info("Starting data transformation...")
        # Validate input data
        # Convert JSON data to DataFrame
        try:
            df = pd.DataFrame(data)
        except ValueError:
            self.logger.error("Invalid JSON data format. Cannot convert to DataFrame.")
            raise ValueError(
                "Invalid JSON data format. Cannot convert to DataFrame."
            ) from None
        # Validate input data columns
        required_columns = [
            "poi_id",
            "poi_name",
            "poi_type",
            "date",
            "time_indicator",
            "total_unique_volume",
            "total_unique_intl_only_visitors",
            "total_unique_domestic_visitors",
            "total_unique_workers",
            "total_unique_residents",
            "avg_dwell_time"
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            self.logger.error(f"Missing required columns: {', '.join(missing_columns)}")
            raise ValueError(
                f"Missing required columns: {', '.join(missing_columns)}"
            ) from None
        # Copy data to avoid modifying the original DataFrame
        transformed_data = df.copy()
        transformed_data["date"] = pd.to_datetime(transformed_data["date"])
        transformed_data = transformed_data.rename(
            columns={"date": "count_date"}
        )
        # Select specific columns
        transformed_data = transformed_data[
            [
                "poi_id",
                "poi_name",
                "poi_type",
                "count_date",
                "time_indicator",
                "total_unique_volume",
                "total_unique_intl_only_visitors",
                "total_unique_domestic_visitors",
                "total_unique_workers",
                "total_unique_residents",
                "avg_dwell_time",
            ]
        ]
        transformed_data.replace("IDE", np.nan, inplace=True)
        transformed_data['total_unique_domestic_visitors'] = transformed_data[
            'total_unique_domestic_visitors'].astype('Int64')
        transformed_data['total_unique_intl_only_visitors'] = transformed_data[
            'total_unique_intl_only_visitors'].astype('Int64')
        transformed_data['total_unique_residents'] = transformed_data[
            'total_unique_residents'].astype('Int64')
        transformed_data['total_unique_volume'] = transformed_data[
            'total_unique_volume'].astype('Int64')
        transformed_data['total_unique_workers'] = transformed_data[
            'total_unique_workers'].astype('Int64')
        transformed_data['avg_dwell_time'] = transformed_data[
            'avg_dwell_time'].astype('Int64')
        transformed_data['poi_id'] = transformed_data[
            'poi_id'].astype('Int64')
        self.logger.info("Data pre-processing completed.")
        return transformed_data
