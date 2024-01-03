import logging

import numpy as np
import pandas as pd

from highstreets.data_source_sink.dataloader import DataLoader


class MsoaTransform(DataLoader):
    def __init__(self):
        """
        MSOA Transform class for performing data transformation on data.

        This class provides methods to transform input data containing HSDS information,
        such as hex_id, date, total_volume, worker_population_percentage,
        and resident_population_percentage, into a desired output format.

        Attributes:
            logger (logging.Logger): Logger instance for logging messages.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler())

    def raw_msoa_transform_data(self, data):
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
            "date",
            "total_volume",
            "worker_population_percentage",
            "resident_population_percentage",
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            self.logger.error(f"Missing required columns: {', '.join(missing_columns)}")
            raise ValueError(
                f"Missing required columns: {', '.join(missing_columns)}"
            ) from None
        # Copy data to avoid modifying the original DataFrame
        transformed_data = df.copy()

        self.logger.info("Removing IDE values from the dataset.")
        self.logger.info(
            "Number of 'IDE' values: %d", transformed_data.isin(["IDE"]).sum().sum()
        )
        self.logger.info(
            "Percentage of 'IDE' values: %.2f%%",
            (transformed_data.isin(["IDE"]).sum().sum() / transformed_data.size) * 100,
        )
        transformed_data.replace("IDE", np.nan, inplace=True)
        # Perform data transformation operations
        transformed_data["date"] = pd.to_datetime(transformed_data["date"])
        transformed_data["worker"] = np.round(
            pd.to_numeric(
                transformed_data["worker_population_percentage"]
                / 100
                * transformed_data["total_volume"],
                errors="coerce",
            )
        ).astype("Int64")
        transformed_data["resident"] = np.round(
            pd.to_numeric(
                transformed_data["resident_population_percentage"]
                / 100
                * transformed_data["total_volume"],
                errors="coerce",
            )
        ).astype("Int64")
        transformed_data["visitor"] = transformed_data["total_volume"] - (
            transformed_data["worker"] + transformed_data["resident"]
        )
        transformed_data["visitor"] = np.round(
            pd.to_numeric(transformed_data["visitor"], errors="coerce")
        ).astype("Int64", errors="ignore")
        transformed_data["day"] = (
            transformed_data["date"].dt.strftime("%a").astype("category")
        )
        transformed_data = transformed_data.rename(
            columns={"poi_id": "msoa_id", "date": "count_date", "poi_name": "msoa_name"}
        )
        transformed_data["msoa_id"] = transformed_data["msoa_id"].astype(str)
        transformed_data["hour"] = transformed_data["hour"].astype(int)
        # Select specific columns
        transformed_data = transformed_data[
            [
                "msoa_id",
                "msoa_name",
                "count_date",
                "day",
                "hour",
                "resident",
                "worker",
                "visitor",
                "loyalty_percentage",
                "dwell_time",
            ]
        ]
        self.logger.info("Data transformation completed.")
        return transformed_data
