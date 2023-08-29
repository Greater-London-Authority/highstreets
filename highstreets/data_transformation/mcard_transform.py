import logging
import re

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
            "//DC1-FILE01/Intelligence$/Projects/2019-20/Covid-19 Busyness/data/"
            "reference_data/mcard_grid_to_ldn_ref_lookup.csv"
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
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/"
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
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/"
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
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/"
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
            "/mnt/q/Projects/2019-20/Covid-19 Busyness/"
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
