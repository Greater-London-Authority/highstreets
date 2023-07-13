import logging

import numpy as np
import pandas as pd

from highstreets.data_source_sink.dataloader import DataLoader


class HexTransform(DataLoader):
    def __init__(self):
        """
        Transform class for performing data transformation on data.

        This class provides methods to transform input data containing HSDS information,
        such as hex_id, date, total_volume, worker_population_percentage,
        and resident_population_percentage, into a desired output format.

        Attributes:
            logger (logging.Logger): Logger instance for logging messages.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler())

    def transform_data(self, data):
        """
        Transforms the given input data.

        Args:
            data (pandas.DataFrame or list or dict): Input data in DataFrame,
            list, or dictionary format.

        Returns:
            pandas.DataFrame: Transformed data with selected columns.

        Raises:
            ValueError: If the input data is invalid or missing required columns.

        Usage:
            hex_transform = HexTransform()
            transformed_data = hex_transform.transform_data(data)
        """
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
        transformed_data["time_indicator"] = transformed_data["time_indicator"].astype(
            "category"
        )
        transformed_data["day"] = (
            transformed_data["date"].dt.strftime("%a").astype("category")
        )
        transformed_data = transformed_data.rename(columns={"poi_id": "hex_id"})
        transformed_data = transformed_data.rename(columns={"date": "count_date"})
        transformed_data["hex_id"] = transformed_data["hex_id"].astype(int)

        # Select specific columns
        transformed_data = transformed_data[
            [
                "hex_id",
                "count_date",
                "day",
                "time_indicator",
                "resident",
                "worker",
                "visitor",
                "loyalty_percentage",
                "dwell_time",
            ]
        ]
        # Convert time_indicator values
        time_indicator_map = {
            "00AM-02AM": "00-03",
            "03AM-05AM": "03-06",
            "06AM-08AM": "06-09",
            "09AM-11AM": "09-12",
            "12PM-14PM": "12-15",
            "15PM-17PM": "15-18",
            "18PM-20PM": "18-21",
            "21PM-23PM": "21-24",
        }

        transformed_data["time_indicator"] = transformed_data["time_indicator"].replace(
            time_indicator_map
        )

        self.logger.info("Data transformation completed.")
        return transformed_data

    def highstreet_threehourly_transform(self, transformed_data):
        Highstreets_quad_lookup = pd.read_csv(
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/"
            "data/mastercard/Highstreets_quad_lookup.csv"
        )
        hex_highstreet_lookup = pd.read_csv(
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/data/reference_data/"
            "hex_highstreet_lookup.csv"
        )
        transformed_data = hex_highstreet_lookup.merge(
            transformed_data, left_on="hex_id", right_on="hex_id", how="right"
        )
        transformed_data = pd.melt(
            transformed_data,
            id_vars=[
                "hex_id",
                "highstreet_id",
                "highstreet_name",
                "count_date",
                "day",
                "time_indicator",
                "loyalty_percentage",
                "dwell_time",
            ],
            var_name="count_type",
            value_name="volume",
        )
        transformed_data = transformed_data.dropna(subset=["highstreet_id"])
        transformed_data["time_indicator"] = transformed_data["time_indicator"].astype(
            str
        )

        transformed_data = (
            transformed_data.groupby(
                [
                    "highstreet_id",
                    "highstreet_name",
                    "count_type",
                    "count_date",
                    "time_indicator",
                ]
            )
            .aggregate(
                volume=("volume", lambda x: round(x.sum(), 2)),
                ave_loyalty_percentage=(
                    "loyalty_percentage",
                    lambda x: round(x.mean(), 2),
                ),
                ave_dwell_time=("dwell_time", lambda x: round(x.mean(), 2)),
            )
            .reset_index()
        )

        transformed_data = transformed_data.pivot_table(
            index=[
                "highstreet_id",
                "highstreet_name",
                "count_date",
                "time_indicator",
                "ave_loyalty_percentage",
                "ave_dwell_time",
            ],
            columns="count_type",
            values="volume",
        ).reset_index()

        transformed_data = transformed_data.merge(
            Highstreets_quad_lookup.drop_duplicates(subset="highstreet_id").loc[
                :, ["highstreet_id", "x", "y", "borough"]
            ],
            on="highstreet_id",
            how="left",
        )
        transformed_data = transformed_data[
            [
                "highstreet_id",
                "highstreet_name",
                "count_date",
                "time_indicator",
                "x",
                "y",
                "borough",
                "resident",
                "visitor",
                "worker",
                "ave_loyalty_percentage",
                "ave_dwell_time",
            ]
        ]
        # Define column data types
        column_types = {
            "highstreet_id": "int64",
            "time_indicator": "category",
            "resident": "int64",
            "visitor": "int64",
            "worker": "int64",
            "highstreet_name": str,
            "borough": str,
        }
        # Convert columns to specified data types
        transformed_data = transformed_data.astype(column_types)
        transformed_data = transformed_data.rename(columns={"time_indicator": "hours"})
        return transformed_data

    def towncentre_threehourly_transform(self, transformed_data):
        TownCentres_quad_lookup = pd.read_csv(
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/"
            "data/mastercard/TownCentres_quad_lookup.csv"
        )
        hex_towncentre_lookup = pd.read_csv(
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/data/reference_data/"
            "hex_towncentre_lookup.csv"
        )
        transformed_data = hex_towncentre_lookup.merge(
            transformed_data, left_on="hex_id", right_on="hex_id", how="right"
        )
        transformed_data = pd.melt(
            transformed_data,
            id_vars=[
                "hex_id",
                "tc_id",
                "tc_name",
                "count_date",
                "day",
                "time_indicator",
                "loyalty_percentage",
                "dwell_time",
            ],
            var_name="count_type",
            value_name="volume",
        )
        transformed_data = transformed_data.dropna(subset=["tc_id"])
        transformed_data["time_indicator"] = transformed_data["time_indicator"].astype(
            str
        )

        transformed_data = (
            transformed_data.groupby(
                ["tc_id", "tc_name", "count_type", "count_date", "time_indicator"]
            )
            .aggregate(
                volume=("volume", lambda x: round(x.sum(), 2)),
                ave_loyalty_percentage=(
                    "loyalty_percentage",
                    lambda x: round(x.mean(), 2),
                ),
                ave_dwell_time=("dwell_time", lambda x: round(x.mean(), 2)),
            )
            .reset_index()
        )

        transformed_data = transformed_data.pivot_table(
            index=[
                "tc_id",
                "tc_name",
                "count_date",
                "time_indicator",
                "ave_loyalty_percentage",
                "ave_dwell_time",
            ],
            columns="count_type",
            values="volume",
        ).reset_index()

        transformed_data = transformed_data.merge(
            TownCentres_quad_lookup.drop_duplicates(subset="tc_id").loc[
                :, ["tc_id", "x", "y", "borough"]
            ],
            on="tc_id",
            how="left",
        )
        transformed_data = transformed_data[
            [
                "tc_id",
                "tc_name",
                "count_date",
                "time_indicator",
                "x",
                "y",
                "borough",
                "resident",
                "visitor",
                "worker",
                "ave_loyalty_percentage",
                "ave_dwell_time",
            ]
        ]
        # Define column data types
        column_types = {
            "tc_id": "int64",
            "time_indicator": "category",
            "resident": "int64",
            "visitor": "int64",
            "worker": "int64",
            "tc_name": str,
            "borough": str,
        }
        # Convert columns to specified data types
        transformed_data = transformed_data.astype(column_types)
        transformed_data = transformed_data.rename(columns={"time_indicator": "hours"})
        return transformed_data

    def bid_threehourly_transform(self, transformed_data):
        BIDS_quad_lookup = pd.read_csv(
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/"
            "data/mastercard/BIDS_quad_lookup.csv"
        )
        hex_bid_lookup = pd.read_csv(
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/data/reference_data/"
            "hex_bid_lookup.csv"
        )
        transformed_data = hex_bid_lookup.merge(
            transformed_data, left_on="hex_id", right_on="hex_id", how="right"
        )
        transformed_data = pd.melt(
            transformed_data,
            id_vars=[
                "hex_id",
                "bid_id",
                "bid_name",
                "count_date",
                "day",
                "time_indicator",
                "loyalty_percentage",
                "dwell_time",
            ],
            var_name="count_type",
            value_name="volume",
        )
        transformed_data = transformed_data.dropna(subset=["bid_id"])
        transformed_data["time_indicator"] = transformed_data["time_indicator"].astype(
            str
        )

        transformed_data = (
            transformed_data.groupby(
                ["bid_id", "bid_name", "count_type", "count_date", "time_indicator"]
            )
            .aggregate(
                volume=("volume", lambda x: round(x.sum(), 2)),
                ave_loyalty_percentage=(
                    "loyalty_percentage",
                    lambda x: round(x.mean(), 2),
                ),
                ave_dwell_time=("dwell_time", lambda x: round(x.mean(), 2)),
            )
            .reset_index()
        )

        transformed_data = transformed_data.pivot_table(
            index=[
                "bid_id",
                "bid_name",
                "count_date",
                "time_indicator",
                "ave_loyalty_percentage",
                "ave_dwell_time",
            ],
            columns="count_type",
            values="volume",
        ).reset_index()

        transformed_data = transformed_data.merge(
            BIDS_quad_lookup.drop_duplicates(subset="bid_id").loc[:, ["bid_id"]],
            on="bid_id",
            how="left",
        )
        transformed_data = transformed_data[
            [
                "bid_id",
                "bid_name",
                "count_date",
                "time_indicator",
                "resident",
                "visitor",
                "worker",
                "ave_loyalty_percentage",
                "ave_dwell_time",
            ]
        ]
        # Define column data types
        column_types = {
            "bid_id": "int64",
            "time_indicator": "category",
            "resident": "int64",
            "visitor": "int64",
            "worker": "int64",
            "bid_name": str,
        }
        # Convert columns to specified data types
        transformed_data = transformed_data.astype(column_types)
        transformed_data = transformed_data.rename(columns={"time_indicator": "hours"})
        return transformed_data

    def bespoke_threehourly_transform(self, transformed_data):
        bespoke_quad_lookup = pd.read_csv(
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/"
            "data/mastercard/bespoke_quad_lookup.csv"
        )
        hex_bespoke_lookup = pd.read_csv(
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/data/reference_data/"
            "hex_bespoke_lookup.csv"
        )
        transformed_data = hex_bespoke_lookup.merge(
            transformed_data, left_on="hex_id", right_on="hex_id", how="right"
        )
        transformed_data = pd.melt(
            transformed_data,
            id_vars=[
                "hex_id",
                "bespoke_area_id",
                "name",
                "count_date",
                "day",
                "time_indicator",
                "loyalty_percentage",
                "dwell_time",
            ],
            var_name="count_type",
            value_name="volume",
        )
        transformed_data = transformed_data.dropna(subset=["bespoke_area_id"])
        transformed_data["time_indicator"] = transformed_data["time_indicator"].astype(
            str
        )

        transformed_data = (
            transformed_data.groupby(
                [
                    "bespoke_area_id",
                    "name",
                    "count_type",
                    "count_date",
                    "time_indicator",
                ]
            )
            .aggregate(
                volume=("volume", lambda x: round(x.sum(), 2)),
                ave_loyalty_percentage=(
                    "loyalty_percentage",
                    lambda x: round(x.mean(), 2),
                ),
                ave_dwell_time=("dwell_time", lambda x: round(x.mean(), 2)),
            )
            .reset_index()
        )

        transformed_data = transformed_data.pivot_table(
            index=[
                "bespoke_area_id",
                "name",
                "count_date",
                "time_indicator",
                "ave_loyalty_percentage",
                "ave_dwell_time",
            ],
            columns="count_type",
            values="volume",
        ).reset_index()

        transformed_data = transformed_data.merge(
            bespoke_quad_lookup.drop_duplicates(subset="bespoke_area_id").loc[
                :, ["bespoke_area_id"]
            ],
            on="bespoke_area_id",
            how="left",
        )
        transformed_data = transformed_data[
            [
                "bespoke_area_id",
                "name",
                "count_date",
                "time_indicator",
                "resident",
                "visitor",
                "worker",
                "ave_loyalty_percentage",
                "ave_dwell_time",
            ]
        ]
        # Define column data types
        column_types = {
            "bespoke_area_id": "int64",
            "time_indicator": "category",
            "resident": "int64",
            "visitor": "int64",
            "worker": "int64",
            "name": str,
        }
        # Convert columns to specified data types
        transformed_data = transformed_data.astype(column_types)
        transformed_data = transformed_data.rename(columns={"time_indicator": "hours"})
        return transformed_data
