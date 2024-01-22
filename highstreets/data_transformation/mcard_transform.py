import logging
import re

import numpy as np
import pandas as pd


class McardTransform:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler())

    def extract_range(self, input_string):
        # Define a regular expression pattern to match the desired range
        pattern = r"\[(\d+-\d+)\)"
        match = re.search(pattern, input_string)
        if match:
            return match.group(1)
        else:
            return None

    def preprocess_mcard_data(self, data):
        mcard_grid_to_ldn_ref_lookup = pd.read_csv(
            "//onelondon.tfl.local/gla/INTELLIGENCE/Projects/2019-20/Covid-19 Busyness/"
            "data/reference_data/mcard_grid_to_ldn_ref_lookup.csv"
        )
        # data manipulation
        data = data[(data["geo_name"] == "London") & (data["segment"] == "Overall")]
        data = data[
            ["quad_id", "txn_date", "time_slot", "txn_amt", "txn_cnt", "avg_spend_amt"]
        ]
        # joining ldn_ref to quad_id
        data = pd.merge(data, mcard_grid_to_ldn_ref_lookup, on="quad_id", how="left")
        data["ldn_ref"] = pd.to_numeric(data["ldn_ref"], errors="coerce").astype(
            pd.Int64Dtype()
        )
        data = data[
            [
                "ldn_ref",
                "quad_id",
                "txn_date",
                "time_slot",
                "txn_amt",
                "txn_cnt",
                "avg_spend_amt",
            ]
        ]
        # Rename columns
        data = data.rename(columns={"txn_date": "count_date", "time_slot": "hours"})

        # Convert 'count_date' column to Date
        data["count_date"] = pd.to_datetime(data["count_date"])

        # Apply the extract_range function to the 'time_indicator' column
        data["hours"] = data["hours"].apply(self.extract_range)

        # Replace '0-3' with '00-03' in the 'time_indicator' column
        data["hours"] = data["hours"].str.replace("0-3", "00-03")

        return data

    def mcard_highstreet_threehourly_transform(self, data):
        Highstreets_quad_lookup = pd.read_csv(
            "//onelondon.tfl.local/gla/INTELLIGENCE/Projects/2019-20/Covid-19 Busyness/"
            "data/mastercard/Highstreets_quad_lookup.csv"
        )
        data = (
            Highstreets_quad_lookup.drop_duplicates(subset="quad_id")
            .merge(data, left_on="quad_id", right_on="quad_id", how="right")
            .dropna(subset=["highstreet_id"])
            .groupby(
                [
                    "highstreet_id",
                    "highstreet_name",
                    "count_date",
                    "hours",
                    "borough",
                    "x",
                    "y",
                ]
            )
            .aggregate(txn_amt=("txn_amt", lambda x: round(x.sum(), 2)))
            .reset_index()
        )
        data["highstreet_id"] = data["highstreet_id"].astype(int)

        return data

    def mcard_towncentre_threehourly_transform(self, data):
        TownCentres_quad_lookup = pd.read_csv(
            "//onelondon.tfl.local/gla/INTELLIGENCE/Projects/2019-20/Covid-19 Busyness/"
            "data/mastercard/TownCentres_quad_lookup.csv"
        )
        data = (
            TownCentres_quad_lookup.drop_duplicates(subset="quad_id")
            .merge(data, left_on="quad_id", right_on="quad_id", how="right")
            .dropna(subset=["tc_id"])
            .groupby(["tc_id", "tc_name", "count_date", "hours", "borough", "x", "y"])
            .aggregate(txn_amt=("txn_amt", lambda x: round(x.sum(), 2)))
            .reset_index()
        )
        data["tc_id"] = data["tc_id"].astype(int)

        return data

    def mcard_bid_threehourly_transform(self, data):
        BIDS_quad_lookup = pd.read_csv(
            "//onelondon.tfl.local/gla/INTELLIGENCE/Projects/2019-20/Covid-19 Busyness/"
            "data/mastercard/BIDS_quad_lookup.csv"
        )
        data = (
            BIDS_quad_lookup.drop_duplicates(subset="quad_id")
            .merge(data, left_on="quad_id", right_on="quad_id", how="right")
            .dropna(subset=["bid_id"])
            .groupby(["bid_id", "bid_name", "count_date", "hours"])
            .aggregate(txn_amt=("txn_amt", lambda x: round(x.sum(), 2)))
            .reset_index()
        )
        data["bid_id"] = data["bid_id"].astype(int)

        return data

    def mcard_bespoke_threehourly_transform(self, data):
        bespoke_quad_lookup = pd.read_csv(
            "//onelondon.tfl.local/gla/INTELLIGENCE/Projects/2019-20/Covid-19 Busyness/"
            "data/mastercard/bespoke_quad_lookup.csv"
        )
        data = (
            bespoke_quad_lookup.merge(
                data, left_on="quad_id", right_on="quad_id", how="right"
            )
            .dropna(subset=["bespoke_area_id"])
            .groupby(["bespoke_area_id", "name", "count_date", "hours"])
            .aggregate(txn_amt=("txn_amt", lambda x: round(x.sum(), 2)))
            .reset_index()
        )
        data["bespoke_area_id"] = data["bespoke_area_id"].astype(int)

        return data

    def calculate_yoy_growth(self, df, col, new_col):
        """
        Calculate Year-over-Year (YOY) growth for a specified column in a DataFrame.

        This method calculates YOY growth for a given column by comparing values with the
        previous year based on matching 'wk' and 'id' columns if 'id' is present.
        It ensures proper sorting of the DataFrame and handles cases where division by
        zero results in infinite values by replacing them with NaN. Finally, it resets
        the index before returning the updated DataFrame.

        Parameters:
            df (pandas.DataFrame): The DataFrame containing the data.
            col (str): The name of the column for which YOY growth will be calculated.
            new_col (str): The name of the new column to store the YOY growth values.

        Returns:
            pandas.DataFrame: The DataFrame with YOY growth values added in the 'new_col'
            column and the index reset.
        """
        try:
            if "id" in df.columns:
                # Ensure 'id' column is treated as an integer
                df["id"] = df["id"].astype(int)
                # Sort the DataFrame by 'id', 'yr', and 'wk' to ensure proper calculation
                df.sort_values(by=["id", "yr", "wk"], inplace=True)

                # Calculate YOY growth based on matching 'wk' and 'id' with
                # the previous year
                df[new_col] = df.groupby(["id", "wk"])[col].shift(0) / df.groupby(
                    ["id", "wk"]
                )[col].shift(1)

                # Set YOY growth to NaN for the first entry of each 'id' and 'wk'
                df.loc[df.groupby(["id", "wk"]).head(1).index, new_col] = None

                # Handle division by zero by replacing resulting infinite values with NaN
                df[new_col].replace([np.inf, -np.inf], np.nan, inplace=True)

                # Sort the DataFrame by 'id', 'yr', and 'wk' to arrange years
                #  in ascending order
                df.sort_values(by=["yr", "wk", "id"], inplace=True)
            else:
                # Sort the DataFrame by 'yr' and 'wk' to ensure proper calculation
                df.sort_values(by=["yr", "wk"], inplace=True)

                # Calculate YOY growth based on matching 'wk' with the previous year
                df[new_col] = df.groupby(["wk"])[col].shift(0) / df.groupby(["wk"])[
                    col
                ].shift(1)

                # Set YOY growth to NaN for the first entry of each 'wk'
                df.loc[df.groupby(["wk"]).head(1).index, new_col] = None

                # Handle division by zero by replacing resulting infinite values with NaN
                df[new_col].replace([np.inf, -np.inf], np.nan, inplace=True)

                # Sort the DataFrame by 'yr', and 'wk' to arrange years
                #  in ascending order
                df.sort_values(by=["yr", "wk"], inplace=True)

            # Reset the index and return the updated DataFrame
            return df.reset_index(drop=True)
        except Exception as e:
            # Handle any exceptions here
            self.logger.error(f"An error occurred: {str(e)}")
