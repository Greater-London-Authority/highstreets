# file_processor.py
import os
import re
import fsspec
import pandas as pd
import numpy as np
from datetime import datetime
from highstreets import config
from sqlalchemy import create_engine, text
from highstreets.core.logger import setup_logger
from highstreets.data_transformation.mcard_transform import McardTransform
from highstreets.core.sql_manager import SQLManager
import logging
from itertools import product


class FileProcessor:
    def __init__(self, data_loader, data_writer, dir_path: str):
        self.data_loader = data_loader
        self.data_writer = data_writer
        self.sectors_df = config.SECTORS_DF
        self.base_dir = config.BASE_DIR
        self.spending_pulse_filepath_raw = config.SP_DIR
        self.spending_pulse_filepath_processed = config.SP_FILEPATH_PROCESSED
        self.mcard_adj_path = config.MCARD_ADJ_PATH
        self.mcard_adj_path1 = config.MCARD_ADJ_PATH1
        self.mcard_adj_path2 = config.MCARD_ADJ_PATH2
        self.mcard_adj_path3 = config.MCARD_ADJ_PATH3
        self.adjustment_factor_dir = config.ADJUSTMENT_FACTOR_DIR
        self.inner_outer_quad_dir = config.INNER_OUTER_QUAD_DIR
        self.dir_path = dir_path
        # Add filesystem support
        self.fs = self._get_filesystem(dir_path)
        self.new_files = []
        self.existing_files = []
        self.sql_manager = SQLManager()
        self.mcard_transform = McardTransform()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        super().__init__()
        self.sql_manager = SQLManager()
        self.logger = setup_logger(__name__)
        self.logger.addHandler(logging.StreamHandler())
        self.database = os.getenv("PG_DATABASE")
        self.username = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASSWORD")
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT")
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
        logging.info("FileProcessor initialized.")

    def _get_filesystem(self, path):
        """Get appropriate filesystem based on path protocol"""
        if path.startswith('s3://'):
            return fsspec.filesystem('s3')
        else:
            return fsspec.filesystem('file')

    @staticmethod
    def extract_date_from_filename(filename: str):
        date_match = re.search(r"_(\d{8})_(\d{8})_", filename)
        if date_match:
            start_date_str = date_match.group(1)
            end_date_str = date_match.group(2)
            start_date = datetime.strptime(start_date_str, "%Y%m%d")
            end_date = datetime.strptime(end_date_str, "%Y%m%d")
            return start_date, end_date
        return None, None

    def create_adjustment_factor(
        self,
        table_name="econ_busyness_mcard_inner_outer_txn_pre_adj",
        rolling_average_months=12,
        ffill_missing_dates=False,
        date_from=None,
        date_to=None,
        update_pg_table=False,
    ):
        """
        Creates adjustment factor using most recent MCard Spending Pulse data and saves
        to csv file
        1. Re-formats both weekly and spending pulse data. Creates monthly values for
        the weekly data
        2. Smooths both datasets using a 12-month rolling average window
        3. Uses the smoothed data to calculate the adjustment factor
        4. Saves output to a csv file

        Parameters
        ----------
        table_name: str
            The name of the spend weekly quad table in the PostgreSQL database.
        spending_pulse_filepath: str
            Filepath to where the most recent spending pulse file is saved.
            These filepaths could be changed to wherever this data is best accessed from.
        sectors_df: pd.DataFrame
            Pre-defined dataframe linking GeoInsights, Spending Pulse and CPI sectors
        rolling_average_months: int
            Number of months for rolling average
        ffill_missing_dates: bool
            Whether to forward fill missing Spending Pulsedates

        Returns
        --------
        csv file with a monthly inner and outer London adjustment factor for
        different sectors (total, eating, apparel)

        """

        # Load data
        logging.info("Starting create_adjustment_factor")
        logging.info("Loading and formatting MC weekly data")
        if table_name == "econ_busyness_mcard_raw_18_zoom":
            _, txn_inner_outer = self.load_and_format_mc_weekly(table_name=table_name)
        elif table_name == "econ_busyness_mcard_inner_outer_txn_pre_adj":
            txn_inner_outer = self.data_loader.get_full_data(table_name)

        logging.info("Formatting and updating spending pulse data")
        self.format_and_update_spending_pulse(
            # spending_pulse_raw,
            # new_spending_pulse_filename,
            self.spending_pulse_filepath_processed
        )
        pulse_all = pd.read_csv(self.spending_pulse_filepath_processed)

        # GeoInsights data formatting
        logging.info("Formatting GeoInsights data")
        txn_inner_outer["week_start"] = pd.to_datetime(txn_inner_outer["week_start"])
        if date_from is not None:
            txn_inner_outer = txn_inner_outer[
                txn_inner_outer["week_start"] >= date_from
            ]
        if date_to is not None:
            txn_inner_outer = txn_inner_outer[txn_inner_outer["week_start"] < date_to]

        txn_inner_outer["inner_outer"] = txn_inner_outer["inner_outer"].replace(
            {"Inner": "Inner London", "Outer": "Outer London"}
        )
        txn_inner_outer = txn_inner_outer.rename(
            columns={"week_start": "startDate", "inner_outer": "geography"}
        )
        txn_inner_outer["month"] = txn_inner_outer["startDate"].dt.month
        txn_inner_outer["yr"] = txn_inner_outer["startDate"].dt.year
        txn_inner_outer = txn_inner_outer.sort_values(by=["geography", "startDate"])

        # Spending Pulse data formatting
        # pulse_all['startDate'] = pd.to_datetime(pulse_all['startDate'],dayfirst=True)
        # pulse_all['startDate'] = pd.to_datetime(pulse_all['startDate'],
        # format='%m/%d/%Y')
        pulse_all["startDate"] = pd.to_datetime(pulse_all["startDate"])
        pulse_all["endDate"] = pd.to_datetime(pulse_all["endDate"])
        if date_from is not None:
            pulse_all = pulse_all[pulse_all["startDate"] >= date_from]
        if date_to is not None:
            pulse_all = pulse_all[pulse_all["startDate"] < date_to]

        # Fill in empty Sales_inStore data with Total Sales column (this is for Grcoery
        # spend because there isn't any InStore data currently)
        pulse_all.loc[
            (pulse_all["sector"] == "Grocery") & (pulse_all["Sales_inStore"].isnull()),
            "Sales_inStore",
        ] = pulse_all["Sales"]

        # Filter only for rows were there's a full month of data
        pulse_all = pulse_all[pulse_all["endDate"].dt.is_month_end]
        sp_max_full_month_date = pulse_all["endDate"].max()
        pulse_all["month"] = pulse_all["startDate"].dt.month
        pulse_all["yr"] = pulse_all["startDate"].dt.year
        pulse_all = pulse_all.sort_values(by=["geography", "sector", "startDate"])

        # Get the sector names
        txn_to_sp_sector_dict = (
            self.sectors_df[["geo_insights", "spending_pulse"]]
            .set_index("geo_insights")
            .T.to_dict("records")[0]
        )
        sectors = self.sectors_df["geo_insights"].to_list()

        # Filter the raw weekly data also to the same date as the SP
        txn_inner_outer = txn_inner_outer[
            txn_inner_outer["startDate"] <= sp_max_full_month_date
        ]

        # Weekly data -> monthly average
        txn_io_monthly = (
            txn_inner_outer.groupby(["geography", "yr", "month"])[
                [f"txn_amt_wd_{i}" for i in sectors]
                + [f"txn_amt_we_{i}" for i in sectors]
            ]
            .mean()
            .reset_index()
        )
        txn_io_monthly = txn_io_monthly.sort_values(by=["geography", "yr", "month"])

        # Create 12-month rolling averages for both datasets
        txn_io_monthly_c, pulse_all_c = self.rolling_average_and_normalise(
            txn_io_monthly,
            pulse_all,
            sectors,
            rolling_average_months=12,
            rolling_av_centered=True,
        )
        txn_io_monthly_nonc, pulse_all_nonc = self.rolling_average_and_normalise(
            txn_io_monthly,
            pulse_all,
            sectors,
            rolling_average_months=12,
            rolling_av_centered=False,
        )
        # This is so the adjustment factor won't change over time
        # it only takes historical data into account

        # Concatenate different types of rolling average
        pulse_all = pd.concat(
            [
                pulse_all_c[pulse_all_c["date"] < "2023-11-01"],
                pulse_all_nonc[pulse_all_nonc["date"] >= "2023-11-01"],
            ]
        )

        txn_io_monthly = pd.concat(
            [
                txn_io_monthly_c[txn_io_monthly_c["date"] < "2023-11-01"],
                txn_io_monthly_nonc[txn_io_monthly_nonc["date"] >= "2023-11-01"],
            ]
        )

        pulse_all.drop(columns=["date"], inplace=True)
        txn_io_monthly.drop(columns=["date"], inplace=True)

        # Re-structure the pulse data
        pulse_pivot = (
            pd.pivot_table(
                pulse_all,
                index=["geography", "yr", "month", "startDate"],
                columns="sector",
                values=f"Sales_inStore_rolling_{rolling_average_months}mo_change_from_2018",  # noqa: E501
            )
        ).reset_index()
        # Merge
        merged = pd.merge(
            txn_io_monthly, pulse_pivot, on=["geography", "yr", "month"], how="left"
        )

        # Calculate the adjustment and add as new column
        for sector in sectors:
            merged[f"adjustment_factor_{sector}"] = (
                merged[
                    f"txn_amt_wd_{sector}_rolling"
                    f"_{rolling_average_months}mo_change_from_2018"
                ]
            ) / (merged[txn_to_sp_sector_dict[sector]])
        merged = merged.sort_values(by=["geography", "yr", "month"])

        merged[[f"adjustment_factor_{i}" for i in sectors]] = merged[
            [f"adjustment_factor_{i}" for i in sectors]
        ].fillna(method="ffill")

        # Format for output / csv file
        adj_factor = merged[
            ["geography", "yr", "month"] + [f"adjustment_factor_{i}" for i in sectors]
        ]
        adj_factor = adj_factor.rename(columns={"geography": "inner_outer"})
        adj_factor["inner_outer"].replace(
            {"Inner London": "Inner", "Outer London": "Outer"}, inplace=True
        )

        # If true
        if ffill_missing_dates:
            # This forward fills the adjustment
            adj_factor["date"] = pd.to_datetime(
                dict(year=adj_factor["yr"], month=adj_factor["month"], day=1)
            )

            # Groupby inner_outer and reindex dates
            adj_factor = (
                adj_factor.groupby("inner_outer", group_keys=False)
                .apply(self.reindex_by_date)
                .reset_index(0, drop=True)
                .reset_index()
            )
            adj_factor["yr"] = adj_factor["index"].dt.year
            adj_factor["month"] = adj_factor["index"].dt.month
            adj_factor.drop(columns=["index"], inplace=True)

        # Truncate and load to Postgres
        adj_factor["date"] = pd.to_datetime(
            dict(year=adj_factor["yr"], month=adj_factor["month"], day=1)
        )
        adj_factor = adj_factor[
            [
                "inner_outer",
                "date",
                "yr",
                "month",
                "adjustment_factor_retail",
                "adjustment_factor_apparel",
                "adjustment_factor_eating",
            ]
        ].round(3)

        # Write to PG table

        if update_pg_table:
            self.data_writer.truncate_and_load_to_postgres(
                adj_factor,
                "econ_busyness_mcard_adjustment_factors",
                schema="gisapdata",
                index=False,
            )

        # Save to CSV
        if date_to is None:
            adj_factor.to_csv(self.mcard_adj_path, index=False)
            adj_factor.to_csv(self.mcard_adj_path1, index=False)
            # adj_factor.to_csv(self.mcard_adj_path2, index=False)
            # adj_factor.to_csv(self.mcard_adj_path3, index=False)

        else:
            adj_factor.to_csv(
                f"Z:/HSDS/data/mastercard/spendingpulse/test/"
                f"mcard_adjustment_factor_"
                f"{''.join(date_to.split('-'))}.csv",
                index=False,
            )

        return (
            adj_factor,
            merged,
            txn_inner_outer,
            txn_io_monthly,
            pulse_all,
            pulse_pivot,
        )

    def get_most_recent_file(self, directory, fs=None):
        """
        Fetches the most recent CSV file from the given directory.

        Args:
            directory (str): The directory containing the CSV files.
            fs (fsspec.AbstractFileSystem, optional): The file system to use.
            Defaults to None.

        Returns:
            str: The path to the most recent CSV file.
        """
        logging.info(f"Getting the most recent file in {directory}")
        try:
            if fs is None:
                fs = fsspec.filesystem("file")

            # List all files in the directory
            files = fs.ls(directory)
            logging.info(f"Found {len(files)} files in the directory.")

            # Regular expression to match the file name pattern
            pattern = re.compile(r"SpendingPulse_London_YTD(\d{4})_(\w+)")

            # Dictionary to store file paths and their corresponding dates
            file_dates = {}

            for file in files:
                file_name = os.path.basename(file)
                match = pattern.match(file_name)
                if match:
                    year = int(match.group(1))
                    month = match.group(2)
                    # Convert month name to month number
                    month_number = datetime.strptime(month, "%B").month
                    file_date = datetime(year, month_number, 1)
                    file_dates[file] = file_date

            if not file_dates:
                logging.error("No files matched the expected pattern.")
                return None

            # Find the most recent file
            most_recent_file = max(file_dates, key=file_dates.get)
            logging.info(f"The most recent file is: {most_recent_file}")

            return most_recent_file

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return None

    def format_and_update_spending_pulse(self, spending_pulse_filepath):
        """
        Parameters
        ---------
        spending_pulse_raw: str
            Filepath to folder containing raw/received spending pulse files
        new_spending_pulse_filename: str
            Filename of the new Spending Pulse file
        spending_pulse_filepath: str
            Filepath to the existing combined Spending Pulse data to be updated
        ----------
        Saves an updated version of Spending Pulse to the filepath specified
        May need to check date formatting in the new data
        """
        fs = (
            fsspec.filesystem("s3")
            if self.spending_pulse_filepath_raw.startswith("s3://")
            else fsspec.filesystem("file")
        )

        most_recent_file = self.get_most_recent_file(
            self.spending_pulse_filepath_raw, fs
        )
        # Read in new data and format date columns
        # Add s3:// prefix if using S3 filesystem
        if self.spending_pulse_filepath_raw.startswith("s3://"):
            most_recent_file = f"s3://{most_recent_file}"

        new_sp = pd.read_csv(most_recent_file)
        new_sp["startDate"] = pd.to_datetime(new_sp["startDate"], dayfirst=True)
        # new_sp['startDate'] = pd.to_datetime(new_sp['startDate'],format='%m/%d/%Y')
        # sometimes this column is in a different format
        new_sp["endDate"] = pd.to_datetime(new_sp["endDate"])
        new_sp = new_sp[new_sp["endDate"].dt.is_month_end]

        # Read in 'master' data and format date columns
        sp = pd.read_csv(spending_pulse_filepath)
        sp["startDate"] = pd.to_datetime(sp["startDate"])
        sp["endDate"] = pd.to_datetime(sp["endDate"])
        sp = sp[sp["endDate"].dt.is_month_end]

        # Get max dates in each
        max_date_current = sp["startDate"].max()
        max_date_new = new_sp["startDate"].max()

        # If max date in new data is more recent than current
        # 'master' data -> concatenate new data
        if max_date_new > max_date_current:
            updated = pd.concat([sp, new_sp[new_sp["startDate"] > max_date_current]])
            updated = updated.sort_values(by=["geography", "sector", "startDate"])
        else:
            updated = sp

        # Save back to 'master' CSV file
        updated.to_csv(spending_pulse_filepath, index=False)

    def reindex_by_date(self, df):
        logging.info("Reindexing by date")
        # For reindexing based on first date of every month until the current month
        dates = pd.date_range(
            df["date"].min(), datetime.today().replace(day=1), freq="MS"
        )
        return df.set_index("date").reindex(dates).ffill()

    # Create 12-month centered rolling average for both datasets
    def rolling_average_and_normalise(
        self,
        txn_io_monthly,
        pulse_all,
        sectors,
        rolling_average_months=12,
        rolling_av_centered=True,
    ):
        """
        Calculate rolling averages and normalise

        Parameters
        ------------
        txn_io_monthly: pd.DataFrame
            Monthly spend df
        pulse_all: pd.DataFrame
            Monthly Spending Pulse df
        sectors: list
            List of sectors
        rolling_average_months: int
            Number of months for rolling average
        rolling_av_centered: bool
            Whether to centre the rolling average

        Returns
        ------------
        DataFrames with normalised rolling average columns

        """
        logging.info("Calculating rolling averages and normalising")
        txn_io_monthly = txn_io_monthly.copy()
        pulse_all = pulse_all.copy()
        wd_cols = [f"txn_amt_wd_{i}" for i in sectors]
        for col in wd_cols + [f"txn_amt_we_{i}" for i in sectors]:
            txn_io_monthly = txn_io_monthly.groupby(
                "geography", group_keys=False
            ).apply(
                lambda x: self.calculate_rolling_average(
                    x, col, rolling_average_months, centered=rolling_av_centered
                )
            )
        pulse_all = pulse_all.groupby(["geography", "sector"], group_keys=False).apply(
            lambda x: self.calculate_rolling_average(
                x, "Sales_inStore", rolling_average_months, centered=rolling_av_centered
            )
        )

        # 2018==1 adjustment on smoothed data
        txn_io_monthly = txn_io_monthly.sort_values(by=["geography", "yr", "month"])
        for col in [
            f"txn_amt_wd_{i}_rolling_{rolling_average_months}mo" for i in sectors
        ] + [f"txn_amt_we_{i}_rolling_{rolling_average_months}mo" for i in sectors]:
            txn_io_monthly = txn_io_monthly.groupby(
                ["geography"], group_keys=False
            ).apply(
                lambda x: self.calculate_change_with_year_average(
                    x, yr_to_average=2018, col=col
                )
            )
        pulse_all = pulse_all.groupby(["geography", "sector"], group_keys=False).apply(
            lambda x: self.calculate_change_with_year_average(
                x,
                yr_to_average=2018,
                col=f"Sales_inStore_rolling_{rolling_average_months}mo",
            )
        )
        pulse_all["date"] = pd.to_datetime(
            dict(year=pulse_all.yr, month=pulse_all.month, day=1)
        )
        txn_io_monthly["date"] = pd.to_datetime(
            dict(year=txn_io_monthly.yr, month=txn_io_monthly.month, day=1)
        )

        return txn_io_monthly, pulse_all

    def calculate_rolling_average(self, df, col, months, centered=True):
        """
        Calculate rolling average of a dataframe column

        Parameters
        -----------
        df: pd.DataFrame
        col: str
            Column name to generate rolling average for
        months: int
            Number of months to use for the rolling average
        centered: bool
            Whether rolling average is centered or not

        Returns
        -----------
        df: pd.DataFrame
            df with additional rolling average column

        """
        df = df.sort_values(by=["yr", "month"])

        if centered:
            if months % 2 == 0:
                # if months is odd
                # Because pandas.rolling() doesn't calculate a centered rolling average
                # correctly with an even size window
                df[f"{col}_rolling_{months}mo_step1"] = (
                    df[col].rolling(months, min_periods=1, center=True).mean()
                )
                df[f"{col}_rolling_{months}mo_step2"] = (
                    df[f"{col}_rolling_{months}mo_step1"]
                    .rolling(2, min_periods=1)
                    .mean()
                    .shift(-1)
                )
                # fill in final value with first step rolling (because you have to shift
                # everything)
                df[f"{col}_rolling_{months}mo_step2"] = np.where(
                    df[f"{col}_rolling_{months}mo_step2"].isnull(),
                    df[f"{col}_rolling_{months}mo_step1"],
                    df[f"{col}_rolling_{months}mo_step2"],
                )
                df = df.rename(
                    columns={
                        f"{col}_rolling_{months}mo_step2": f"{col}_rolling_{months}mo"
                    }
                )
                df.drop(columns=[f"{col}_rolling_{months}mo_step1"], inplace=True)
            else:  # if odd
                df[f"{col}_rolling_{months}mo"] = (
                    df[col].rolling(months, min_periods=1, center=True).mean()
                )
        else:
            df[f"{col}_rolling_{months}mo"] = (
                df[col].rolling(months, min_periods=1).mean()
            )
        return df

    def calculate_change_with_year_average(self, df, yr_to_average=2018, col="Sales"):
        """
        Normalise DF column so that 1 = the monthly average of a specified year

        Parameters
        -----------
        df: pd.DataFrame
        yr_to_average: int
        col: str
            Name of column to normalise

        Returns
        -----------
        df: pd.DataFrame
            df with additional normalised column

        """

        df[f"{col}_change_from_{yr_to_average}"] = (
            df[col] / df[(df["yr"] == yr_to_average)][col].mean()
        )
        # divide by monthly average of the yr specified

        return df

    def load_and_format_mc_weekly(self, table_name="econ_busyness_mcard_raw_18_zoom"):
        """
        Loads most recent raw weekly data and converts into the same format as processed
        weekly files (keeps total retail, eating and apparel industries)
        Generates quad and inner/outer level datasets for txn_amt

        Parameters
        -----------
        table_name: str
            The name of the table in the PostgreSQL database.
        sectors_df: pd.DataFrame
            DataFrame with sector names and relationships between the different datasets

        Returns
        --------
        mcard_weekly_quad: pd.Dataframe
            Formatted txn_amt data for weekday/weekend total retail, eating and apparel
            at quad level
        mcard_weekly_io: pd.Dataframe
            Formatted txn_amt data for weekday/weekend total retail, eating and apparel
            at inner/outer London level (includes most recent data and will be used for
            Spending Pulse adjustment calculation)

        """
        # Finds the most recent week in inner-outer csv
        txn_inner_outer = self.data_loader.get_full_data(
            "econ_busyness_mcard_inner_outer_txn_pre_adj"
        )
        txn_inner_outer["week_start"] = pd.to_datetime(txn_inner_outer["week_start"])
        txn_inner_outer["month"] = txn_inner_outer["week_start"].dt.month
        txn_inner_outer = txn_inner_outer[
            ((txn_inner_outer["yr"] == 2024) & (txn_inner_outer["wk"] <= 47))
            | (txn_inner_outer["yr"] < 2024)
        ]
        # io_max_yr = txn_inner_outer["yr"].max()
        # io_max_wk = txn_inner_outer[txn_inner_outer["yr"] == io_max_yr]["wk"].max()

        # Loads and formats the weekly quad data (gets the most recent data only)
        industrys = tuple(list(self.sectors_df["geo_insights_raw"].values))

        mcard_weekly_quad = self.data_loader.get_partial_data(
            table_name,
            columns=(
                "geo_name, segment, yr, wk, quad_id, weekday_weekend,"
                "industry, txn_amt"
            ),
            where_clause=f"((yr >= {2024} AND wk > {47})"
            f" OR (yr>{2024})) AND industry IN{industrys}"
            f" AND segment = 'Overall' AND geo_name='London'",
        )

        if mcard_weekly_quad is not None:
            mcard_weekly_quad[["yr", "wk"]] = mcard_weekly_quad[["yr", "wk"]].astype(
                int
            )
            # convert to a date column. The additional '1' sets the date as a Monday
            Yw = (
                mcard_weekly_quad["yr"].astype(str)
                + mcard_weekly_quad["wk"].astype(str)
                + "1"
            )
            mcard_weekly_quad["week_start"] = pd.to_datetime(Yw, format="%G%V%w")
            mcard_weekly_quad["quad_id"] = mcard_weekly_quad["quad_id"].astype("Int64")

            # Pivot -> we/wd and industries are in separate columns
            raw_to_weekly_sector_dict = (
                self.sectors_df[["geo_insights_raw", "geo_insights"]]
                .set_index("geo_insights_raw")
                .T.to_dict("records")[0]
            )
            mcard_weekly_quad["industry"].replace(
                raw_to_weekly_sector_dict, inplace=True
            )
            mcard_weekly_quad["weekday_weekend"].replace(
                {"weekdays": "wd", "weekends": "we", "weekday": "wd", "weekend": "we"},
                inplace=True,
            )
            mcard_weekly_quad = mcard_weekly_quad.pivot_table(
                index=["yr", "wk", "week_start", "quad_id"],
                columns=["weekday_weekend", "industry"],
                values="txn_amt",
            )
            mcard_weekly_quad.columns = [
                "txn_amt_" + "_".join(col).strip()
                for col in mcard_weekly_quad.columns.values
            ]
            mcard_weekly_quad.reset_index(inplace=True)

            # Aggregate to inner/outer level
            inner_outer_quad = self.data_loader.get_full_data(
                "econ_busyness_mcard_Inner_Outer_quad_lookup"
            )
            inner_outer_quad["quad_id"] = inner_outer_quad["quad_id"].astype("Int64")
            # inner_outer_quad = inner_outer_quad.sort_values(
            #     by="inner_outer"
            # ).drop_duplicates(subset="quad_id", keep="last")
            # where a quad is assigned both Inner and Outer - keep Outer
            mcard_weekly_io = pd.merge(
                mcard_weekly_quad, inner_outer_quad, on=["quad_id"], how="inner"
            )
            mcard_weekly_io = (
                mcard_weekly_io.groupby(["yr", "wk", "week_start", "inner_outer"])[
                    [i for i in mcard_weekly_io.columns if i.startswith("txn_amt")]
                ]
                .sum(min_count=1)
                .reset_index()
            )

            # Append new mcard_weekly_io to txn_inner_outer
            mcard_weekly_io = pd.concat([txn_inner_outer, mcard_weekly_io])
        else:
            mcard_weekly_io = txn_inner_outer.copy()

        mcard_weekly_io = mcard_weekly_io[
            ["yr", "wk", "week_start", "inner_outer"]
            + [
                i
                for i in mcard_weekly_io.columns
                if i.startswith("txn_amt_") and not i.endswith("_adj")
            ]
        ]

        return mcard_weekly_quad, mcard_weekly_io

    @staticmethod
    def is_newer_than_recent(file_start_date, recent_year, recent_week) -> bool:
        file_year, file_week = file_start_date.isocalendar()[:2]
        return (file_year > recent_year) or (
            file_year == recent_year and file_week > recent_week
        )

    def process_mcard_raw_files(self, table_name_map: dict):
        recent_dates = self.data_loader.get_most_recent_dates(table_name_map.values())

        # Use fsspec to list files (works for both local and S3)
        if self.dir_path.startswith('s3://'):
            files = self.fs.glob(f"{self.dir_path}*.csv")
            # Add s3:// prefix if needed
            files = [f"s3://{file}" if not file.startswith("s3://") else file for file in files] # noqa
        else:
            files = self.fs.glob(os.path.join(self.dir_path, "*.csv"))

        for file in files:
            zoom_level_match = re.search(r"(\d+)_zoom", file.lower())
            if zoom_level_match:
                zoom_level = zoom_level_match.group(1)
                table_name = table_name_map.get(zoom_level)
                if not table_name:
                    logging.warning(
                        f"No table name found for zoom level {zoom_level}."
                        f" Skipping file {file}."
                    )
                    continue

                file_start_date, _ = self.extract_date_from_filename(file)

                if table_name in recent_dates:
                    recent_year, recent_week = recent_dates[table_name]
                    if not self.is_newer_than_recent(
                        file_start_date, recent_year, recent_week
                    ):
                        self.existing_files.append(os.path.basename(file))
                        logging.info(
                            f"File {file} already present in the database."
                            f" Skipping..."
                        )
                        continue

                self._process_file(file, table_name)

        self._log_results()

    def clean_and_process_data(
        self, zoom, cols, clean_table_name, segment="Overall", geo_name="London"
    ):
        # Reuse the get_most_recent_dates function to check for existing data
        recent_dates = self.data_loader.get_most_recent_dates([clean_table_name])

        if clean_table_name in recent_dates:
            last_yr, last_wk = recent_dates[clean_table_name]
            print(
                f"Existing data found in {clean_table_name}."
                f" Processing raw data from year {last_yr}, week {last_wk}."
            )
        else:
            last_yr, last_wk = None, None
            print(
                f"No existing data in {clean_table_name}." f" Processing all raw data."
            )

        # Retrieve raw data
        df_raw = (
            self.data_loader.query_mcard_raw_since(
                zoom, cols, last_yr, last_wk, segment, geo_name
            )
            if last_yr
            else self.data_loader.query_mcard_weekly_raw(zoom, cols)
        )

        if df_raw.empty:
            print(
                f"Most recent data already exists in {clean_table_name}."
                f" No new data to process."
            )
            return

        # First, let's check the raw counts before any processing
        raw_counts = df_raw[df_raw["txn_amt"] != 0].groupby("industry").size()
        print("Original raw counts per industry:")
        print(raw_counts)

        # Clean the data
        dates = df_raw[["yr", "wk", "weekday_weekend"]].drop_duplicates()
        dates["yr"] = dates["yr"].astype(int)
        dates["wk"] = dates["wk"].astype(int)
        dates["week_start"] = dates.apply(
            lambda row: self.data_loader.get_week_start(row["yr"], row["wk"]), axis=1
        )

        # Get all quads from both the lookup table AND the raw data
        locs = self.data_loader.get_full_data(
            "econ_busyness_mcard_quad_coordinates_lookup"
        )
        lookup_quads = set(locs["quad_id"].unique())
        raw_quads = set(df_raw["quad_id"].unique())

        # Check if there are quads in raw data but not in lookup
        missing_quads = raw_quads - lookup_quads
        if missing_quads:
            print(
                f"WARNING: Found {len(missing_quads)} quad_ids in raw"
                f" data that don't exist in lookup table"
            )

        # Use ALL quads from both sources
        all_quads = list(lookup_quads.union(raw_quads))

        # Apply your specific data filters
        files = df_raw["file_name"].unique()
        if any("12Apr2021_18Apr2021" in file for file in files):
            df_raw = df_raw[
                ~(
                    (df_raw["yr"] == 2021)
                    & (df_raw["wk"] == 15)
                    & df_raw["file_name"].str.contains("05Apr2021_02May2021")
                )
            ]
        df_raw = df_raw[
            ~(
                (df_raw["yr"] == 2020)
                & (df_raw["wk"] == 49)
                & df_raw["file_name"].str.contains("Nov")
            )
        ]

        # After filtering, check counts again
        filtered_counts = df_raw[df_raw["txn_amt"] != 0].groupby("industry").size()
        print("Counts after filtering:")
        print(filtered_counts)

        # Get all unique values for other dimensions
        all_years = df_raw["yr"].unique()
        all_weeks = df_raw["wk"].unique()
        all_industries = df_raw["industry"].unique()
        all_weekday_weekend = df_raw["weekday_weekend"].unique()

        print(
            f"Creating complete grid with"
            f" {len(all_years)} years × {len(all_weeks)} weeks × "
            f"{len(all_industries)} industries × {len(all_quads)} quads × "
            f"{len(all_weekday_weekend)} weekday/weekend options"
        )

        # Complete missing combinations and fill with zeros
        df_clean = df_raw.drop(
            columns=["file_name", "central_latitude", "central_longitude"]
        )

        # Create a complete cartesian product of all dimension combinations
        batch_size = 10000  # Adjust based on memory constraints
        complete_dfs = []

        quad_batches = [
            all_quads[i : i + batch_size] for i in range(0, len(all_quads), batch_size)
        ]
        for quad_batch in quad_batches:
            batch_combinations = list(
                product(
                    all_years,
                    all_weeks,
                    all_industries,
                    quad_batch,
                    all_weekday_weekend,
                )
            )

            batch_df = pd.DataFrame(
                batch_combinations,
                columns=["yr", "wk", "industry", "quad_id", "weekday_weekend"],
            )
            complete_dfs.append(batch_df)

        # Combine all batches
        complete_df = pd.concat(complete_dfs, ignore_index=True)

        # Merge with the original data to get values where they exist
        df_complete = complete_df.merge(
            df_clean,
            on=["yr", "wk", "industry", "quad_id", "weekday_weekend"],
            how="left",
        )

        # Fill NaN values with zeros for numeric columns
        numeric_cols = df_complete.select_dtypes(include=["number"]).columns
        df_complete[numeric_cols] = df_complete[numeric_cols].fillna(0)

        # Merge with dates
        df_complete["yr"] = df_complete["yr"].astype("Int64")
        df_complete["wk"] = df_complete["wk"].astype("Int64")
        df_complete = df_complete.merge(
            dates, on=["yr", "wk", "weekday_weekend"], how="left"
        )

        # Merge with location data (including all quads from lookup table)
        # Use left join to keep all records even if location data is missing
        df_complete = df_complete.merge(locs, on="quad_id", how="left")

        # Remove any duplicates that might have been introduced during processing
        before_dedup = len(df_complete)
        df_complete = df_complete.drop_duplicates(
            subset=["yr", "wk", "industry", "quad_id", "weekday_weekend"], keep="first"
        )
        after_dedup = len(df_complete)

        if before_dedup > after_dedup:
            print(f"Removed {before_dedup - after_dedup} duplicate records")

        # Record counts per industry in the complete dataset
        completeness_count = df_complete.groupby("industry").size()
        print("Record counts per industry (should all be identical):")
        print(completeness_count)

        # Final check for non-zero transactions
        final_counts = (
            df_complete[df_complete["txn_amt"] != 0].groupby("industry").size()
        )
        print("Final counts for non-zero transactions:")
        print(final_counts)

        # Verify non-zero counts match filtered data
        if not filtered_counts.equals(final_counts):
            print(
                "WARNING: Record counts do not match"
                " between filtered raw and final data!"
            )
            print("Differences:")
            diff = pd.DataFrame(
                {"filtered": filtered_counts, "final": final_counts}
            ).fillna(0)
            diff["difference"] = diff["final"] - diff["filtered"]
            print(diff[diff["difference"] != 0])

        print("Data cleaned successfully.")
        # Write the cleaned data to the database
        self.data_writer.append_chunk(df_complete, clean_table_name)

    def _process_file(self, file: str, table_name: str):
        day_end = "weekend" if "weekend" in file.lower() else "weekday"
        chunk_size = 200000
        # table_name = f"test_econ_busyness_mcard_raw_{zoom_level}_zoom"

        # Use fsspec to open file (works for both local and S3)
        with self.fs.open(file, 'rb') as f:
            reader = pd.read_csv(
                f,
                chunksize=chunk_size,
                delimiter="|",
                dtype={
                    "yr": float,
                    "wk": float,
                    "industry": str,
                    "segment": str,
                    "geo_type": str,
                    "geo_name": str,
                    "quad_id": str,
                    "central_latitude": float,
                    "central_longitude": float,
                    "bounding_box": str,
                    "txn_amt": float,
                    "txn_cnt": float,
                    "acct_cnt": float,
                    "avg_ticket": float,
                    "avg_freq": float,
                    "avg_spend_amt": float,
                    "yoy_txn_amt": str,
                    "yoy_txn_cnt": str,
                },
            )

            for i, chunk in enumerate(reader):
                chunk["weekday_weekend"] = (
                    "weekends" if day_end == "weekend" else "weekdays"
                )
                chunk["file_name"] = os.path.basename(file)
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

    def calculate_yoy_growth_compared_to_2019(
        self, df, col, new_col, ids=["inner_outer"]
    ):
        """
        Calculate Year-over-Year (YOY) growth for a specified column in a DataFrame.

        This method calculates YOY growth for a given column by comparing values with
        2019 (2019 is compared with 2018)
         based on matching 'wk' and 'id' columns if 'id' is present.
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
        df = df.copy()
        try:
            if len(ids) > 0:
                # Ensure 'id' column is treated as an integer
                # df["id"] = df["id"].astype(int)
                # Sort the DataFrame by 'id', 'yr', and 'wk' to ensure proper calculation
                df.sort_values(by=ids + ["yr", "wk"], inplace=True)
                # df[new_col] = None
                df_2019 = df[df["yr"].isin([2018, 2019])]
                df_2019[new_col] = df_2019[col].div(
                    df_2019.groupby(ids + ["wk"])[col].transform("first")
                )

                df_2019_onwards = df[df["yr"] != 2018]
                df_2019_onwards[new_col] = df_2019_onwards[col].div(
                    df_2019_onwards.groupby(ids + ["wk"])[col].transform("first")
                )
                df = pd.concat(
                    [df_2019, df_2019_onwards[df_2019_onwards["yr"] != 2019]], axis=0
                )
                df.sort_values(by=ids + ["yr", "wk"], inplace=True)

                # Set YOY growth to NaN for the first entry of each 'id' and 'wk'
                df.loc[df.groupby(ids + ["wk"]).head(1).index, new_col] = None
                # Handle division by zero by replacing resulting infinite values with NaN
                df[new_col].replace([np.inf, -np.inf], np.nan, inplace=True)
                # Sort the DataFrame by 'id', 'yr', and 'wk' to arrange years
                #  in ascending order
                df.sort_values(by=["yr", "wk"] + ids, inplace=True)
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
            logging.info(f"An error occurred: {str(e)}")

    # MASTERCARD Spending Pulse adjustment factor
    def mcard_adjust_weekly(
        self,
        spend,
        lookup_file,
        quad_lookup_file,
        save_path,
        col_to_adjust=["txn_amt"],
        date_col="count_date",
        poi_id=["quad_id"],
        filename="txn",
        cached_inner_outer_quad=None,
        cached_adjustment_factors=None,
        cached_cpi_data=None,
    ):
        """
        Adjusts spend column by monthly correction factor generated from
        Spending Pulse data.
        The adjustment takes into account the cash-to-card shift and the mastercard
        share of the market.
        Sector-specific inflation is also adjusted for.

        Parameters
        ----------
        spend: spend table of MC 3-hourly/weekly data at quad level
        col_to_adjust: name of spend column to be adjusted (e.g. txn_amt (3-hourly))
        adj_col: name of adjustment factor column
                 (sector-specific: adjustment_factor_retail / adjustment_factor_eating /
                adjustment_factor_apparel)
        date_col: name of date column (count_date (3-hourly) or week_start (weekly))
        sectors_df: pre-defined dataframe linking GeoInsights, Spending Pulse and
                    CPI sectors

        Returns
        --------
        Dataframe with additional adjusted spend column (e.g. txn_amt_adj)
        """

        # Add month and yr column to spend data
        spend[date_col] = pd.to_datetime(spend[date_col])
        spend["month"] = spend[date_col].dt.month
        spend["yr"] = spend[date_col].dt.year

        if lookup_file != quad_lookup_file:
            # Merge quad lookup to spend
            quad_lookup = self.data_loader.get_full_data(quad_lookup_file)
            quad_lookup["quad_id"] = quad_lookup["quad_id"].astype("Int64")
            quad_lookup = quad_lookup[["quad_id"] + poi_id]

            # Load quad inner_outer lookup (use cache if available)
            if cached_inner_outer_quad is not None:
                inner_outer_quad = cached_inner_outer_quad.copy()
            else:
                inner_outer_quad = self.data_loader.get_full_data(lookup_file)
            inner_outer_quad["quad_id"] = inner_outer_quad["quad_id"].astype("Int64")

            # where a quad is assigned both Inner and Outer - keep Outer
            inner_outer_quad = inner_outer_quad.sort_values(
                by="inner_outer"
            ).drop_duplicates(subset="quad_id", keep="last")
            # Add inner_outer to spend data
            inner_outer_poi = pd.merge(
                quad_lookup[["quad_id"] + poi_id],
                inner_outer_quad,
                how="left",
                on="quad_id",
            )

            # Designate Inner or Outer to each POI - take most common POI
            poi_io_lookup = (
                inner_outer_poi.groupby(poi_id)["inner_outer"]
                .agg(lambda x: x.mode()[0])
                .reset_index()
            )

            # Add IO to poi lookup to spend data
            poi_io_lookup[poi_id] = poi_io_lookup[poi_id].astype(str)
            spend[poi_id] = spend[poi_id].astype(str)
            spend = pd.merge(spend, poi_io_lookup, how="left", on=poi_id)

        # Join spend data with mcard adjustment data (use cache if available)
        if cached_adjustment_factors is not None:
            adjustment_factor = cached_adjustment_factors.copy()
        else:
            adjustment_factor = self.data_loader.get_full_data(
                "econ_busyness_mcard_adjustment_factors"
            )

        # need to merge on inner vs outer too
        spend = pd.merge(
            spend,
            adjustment_factor,
            how="left",
            left_on=["yr", "month", "inner_outer"],
            right_on=["yr", "month", "inner_outer"],
        )

        # # If spend data is more recent than Spending Pulse, there will be NaNs.
        # # Fill them with the latest available mcard_adjustment
        # spend = spend.sort_values(by=["inner_outer", date_col])
        # for adj_col in [
        #     i for i in spend.columns if i.startswith("adjustment_factor_")]:
        #     spend[adj_col] = spend[adj_col].fillna(method="ffill")

        # Adjust for cash-to-card shift and MC market share - create an additional column
        txn_cat_adj_dict = (
            config.SECTORS_DF[["geo_insights", "adjustment_factor"]]
            .set_index("geo_insights")
            .T.to_dict("records")[0]
        )

        if (len(col_to_adjust) == 1) & (col_to_adjust[0] == "txn_amt"):
            for col in col_to_adjust:
                spend[col + "_adj"] = spend[col] / spend["adjustment_factor_retail"]
        else:
            for sector in txn_cat_adj_dict.keys():
                spend[f"txn_amt_wd_{sector}" + "_adj"] = (
                    spend[f"txn_amt_wd_{sector}"]
                    / spend[f"adjustment_factor_{txn_cat_adj_dict[sector]}"]
                )
                spend[f"txn_amt_we_{sector}" + "_adj"] = (
                    spend[f"txn_amt_we_{sector}"]
                    / spend[f"adjustment_factor_{txn_cat_adj_dict[sector]}"]
                )

        if poi_id != ["inner_outer"]:
            spend.drop(
                columns=["inner_outer"]
                + [i for i in spend.columns if i.startswith("adjustment_factor_")],
                inplace=True,
            )  # remove unecessary columns
        else:
            spend.drop(
                columns=[
                    i for i in spend.columns if i.startswith("adjustment_factor_")
                ],
                inplace=True,
            )  # remove unecessary columns

        # Adjust for inflation (using subcategory-specific CPI)
        # import ONS's CPIH table via API
        # api_client = APIClient()
        # cpi_table = api_client.fetch_cpi()
        if cached_cpi_data is not None:
            cpi_table = cached_cpi_data.copy()
        else:
            cpi_table = self.data_loader.get_full_data("econ_busyness_mcard_cpi_data")
        txn_cat_cpi_dict = (
            self.sectors_df[["geo_insights", "cpi"]]
            .set_index("geo_insights")
            .T.to_dict("records")[0]
        )

        if (len(col_to_adjust) == 1) & (col_to_adjust[0] == "txn_amt"):
            for col in col_to_adjust:
                txn_cat = "retail"
                spend = self.mcard_transform.inflation_adjust(
                    spend,
                    cpi_table[cpi_table["aggregate"] == txn_cat_cpi_dict[txn_cat]][
                        ["yr", "month", "aggregate", "cpi_index"]
                    ],
                    reindexing_year=2018,
                    col_to_adjust=[col + "_adj"],
                    date_col=date_col,
                )

            spend[col + "_adj"] = spend[col + "_adj"].round(3)

        else:
            for sector in txn_cat_adj_dict.keys():
                txn_cat = sector  # col.split('_')[-1] # eating / apparel / retail
                # adjust for inflation (subcat specific CPI) - updates adjusted column
                spend = self.mcard_transform.inflation_adjust(
                    spend,
                    cpi_table[cpi_table["aggregate"] == txn_cat_cpi_dict[txn_cat]][
                        ["yr", "month", "aggregate", "cpi_index"]
                    ],
                    reindexing_year=2018,
                    col_to_adjust=[
                        f"txn_amt_wd_{sector}_adj",
                        f"txn_amt_we_{sector}_adj",
                    ],
                    date_col=date_col,
                )
                spend[f"txn_amt_wd_{sector}_adj"] = spend[
                    f"txn_amt_wd_{sector}_adj"
                ].round(3)
                spend[f"txn_amt_we_{sector}_adj"] = spend[
                    f"txn_amt_we_{sector}_adj"
                ].round(3)

        spend["yr"] = spend[date_col].dt.isocalendar().year
        spend.drop(columns=["month"], inplace=True)  # remove unecessary columns

        def flatten_comprehension(matrix):
            return [item for row in matrix for item in row]

        # Keep only the sectors we're interested in keeping
        spend = spend[
            ["yr", "wk", "week_start"]
            + poi_id
            + flatten_comprehension(
                [
                    [
                        f"txn_amt_wd_{sector}",
                        f"txn_amt_we_{sector}",
                        f"txn_amt_wd_{sector}_adj",
                        f"txn_amt_we_{sector}_adj",
                        f"txn_cnt_wd_{sector}",
                        f"txn_cnt_we_{sector}",
                    ]
                    for sector in config.SECTORS_DF["geo_insights"].values
                ]
            )
        ].sort_values(by=["yr", "wk"] + poi_id)

        # Filepath while testing
        spend.to_csv(f"{save_path}{filename}.csv", index=False)

        # spend.to_csv(
        #     f"{self.base_dir}mastercard/weekly/processed/{filename}.csv", index=False
        # )

        # Adding in a function to calculate YoY
        yoy = spend.copy()

        for col in [i for i in yoy.columns if i.startswith("txn_")]:
            yoy = self.calculate_yoy_growth_compared_to_2019(
                yoy, col, f"yoy_{col}", ids=poi_id
            )

        # -----------------------------------------------------------------------------
        # Additional code to get YOY file in correct format - before saving to CSV
        # -----------------------------------------------------------------------------

        # Filter for 2019 onwards
        yoy = yoy[yoy["yr"] >= 2019]

        # For HS / TC  areas add in centroid x,y and borough columns
        if len(poi_id) > 1:
            if poi_id[1] == "highstreet_name":
                # Get borough for each area

                query = "select * from econ_busyness_mcard_Highstreets_quad_lookup"

                hs_bor_lookup = pd.read_sql(text(query), self.engine.connect())

                hs_bor_lookup = hs_bor_lookup[
                    ["highstreet_id", "highstreet_name", "x", "y", "borough"]
                ].drop_duplicates()
                hs_bor_lookup["highstreet_id"] = hs_bor_lookup["highstreet_id"].astype(
                    "Int64"
                )

                yoy["highstreet_id"] = yoy["highstreet_id"].astype("Int64")

                yoy = pd.merge(
                    yoy,
                    hs_bor_lookup,
                    on=["highstreet_id", "highstreet_name"],
                    how="left",
                )

            elif poi_id[1] == "tc_name":

                query = "select * from econ_busyness_mcard_TownCentres_quad_lookup"

                tc_bor_lookup = pd.read_sql(text(query), self.engine.connect())

                tc_bor_lookup = tc_bor_lookup[
                    ["tc_id", "tc_name", "x", "y", "borough"]
                ].drop_duplicates()
                tc_bor_lookup["tc_id"] = tc_bor_lookup["tc_id"].astype("int64")

                yoy["tc_id"] = yoy["tc_id"].astype("Int64")

                yoy = pd.merge(yoy, tc_bor_lookup, on=["tc_id", "tc_name"], how="left")

            # Get all columns before txn_amt ones
            first_txn_amt_col = [
                column for column in yoy.columns if column.startswith("txn_amt_")
            ][0]
            id_cols = list(yoy.loc[:, :first_txn_amt_col].columns[:-1])

            # Drop txn_ cols
            yoy = yoy.drop(
                columns=[i for i in yoy.columns if i.startswith("txn_")]
            ).round(3)

            # Put columns in correct format/order

            if poi_id[1] == "highstreet_name" or poi_id[1] == "tc_name":

                yoy = yoy[
                    id_cols
                    + ["x", "y", "borough"]
                    + [
                        "yoy_txn_amt_wd_eating",
                        "yoy_txn_amt_we_eating",
                        "yoy_txn_amt_wd_apparel",
                        "yoy_txn_amt_we_apparel",
                        "yoy_txn_amt_wd_retail",
                        "yoy_txn_amt_we_retail",
                        "yoy_txn_cnt_wd_eating",
                        "yoy_txn_cnt_we_eating",
                        "yoy_txn_cnt_wd_apparel",
                        "yoy_txn_cnt_we_apparel",
                        "yoy_txn_cnt_wd_retail",
                        "yoy_txn_cnt_we_retail",
                        "yoy_txn_amt_wd_eating_adj",
                        "yoy_txn_amt_we_eating_adj",
                        "yoy_txn_amt_wd_apparel_adj",
                        "yoy_txn_amt_we_apparel_adj",
                        "yoy_txn_amt_wd_retail_adj",
                        "yoy_txn_amt_we_retail_adj",
                    ]
                ]

            else:
                yoy = yoy[
                    id_cols
                    + [
                        "yoy_txn_amt_wd_eating",
                        "yoy_txn_amt_we_eating",
                        "yoy_txn_amt_wd_apparel",
                        "yoy_txn_amt_we_apparel",
                        "yoy_txn_amt_wd_retail",
                        "yoy_txn_amt_we_retail",
                        "yoy_txn_cnt_wd_eating",
                        "yoy_txn_cnt_we_eating",
                        "yoy_txn_cnt_wd_apparel",
                        "yoy_txn_cnt_we_apparel",
                        "yoy_txn_cnt_wd_retail",
                        "yoy_txn_cnt_we_retail",
                        "yoy_txn_amt_wd_eating_adj",
                        "yoy_txn_amt_we_eating_adj",
                        "yoy_txn_amt_wd_apparel_adj",
                        "yoy_txn_amt_we_apparel_adj",
                        "yoy_txn_amt_wd_retail_adj",
                        "yoy_txn_amt_we_retail_adj",
                    ]
                ]
        # -----------------------------------------------------------------------------------------

        yoy.to_csv(f"{save_path}yoy{filename.split('txn')[1]}.csv", index=False)
        #
        #  yoy.to_csv(
        #     f"{self.base_dir}mastercard/weekly/processed"
        #     f"/yoy{filename.split('txn')[1]}.csv",
        #     index=False,
        # )

        return spend

    def get_inner_outer_weekly_summary(self, since_date=None):
        """
        Gets weekly aggregated transaction data by inner/outer London areas.

        Efficiently queries data from econ_busyness_mcard_clean_18_zoom joined with
        inner/outer lookup table, with specific industry filtering and pivoting.

        Parameters
        ----------
        since_date: str, optional
            Optional date string in format 'YYYY-MM-DD' to filter data since a
            specific date

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: week_start, inner_outer, txn_amt_wd_retail,
            txn_amt_wd_eating, txn_amt_wd_apparel, txn_amt_we_retail,
            txn_amt_we_eating, txn_amt_we_apparel
        """
        query = self.sql_manager.get_query(
            "inner_outer_weekly_summary", "mcard/weekly")

        # Add date filter if provided
        if since_date:
            # Convert since_date to ISO year and week
            since_date_dt = pd.to_datetime(since_date)
            iso_calendar = since_date_dt.isocalendar()
            filter_year = iso_calendar[0]
            filter_week = iso_calendar[1]

            # Create a filter based on yr and wk columns instead of week_start
            filter_clause = f"""
            AND ((c.yr = {filter_year} AND c.wk >= {filter_week})
            OR (c.yr > {filter_year}))
            """

            # Add the filter to the query
            query = query.replace(
                "WHERE ",
                f"WHERE {filter_clause} AND "
            )

        # Execute the query
        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn)

        # Convert week_start to datetime for easier manipulation
        df["week_start"] = pd.to_datetime(df["week_start"])

        # Add month column for potential monthly aggregation
        df["month"] = df["week_start"].dt.month
        df["yr"] = df["week_start"].dt.year

        self.logger.info(
            f"Retrieved {len(df)} rows of inner/outer" f" weekly transaction data"
        )
        return df

    def process_inner_outer_weekly_summary(
            self,
            target_table='econ_busyness_mcard_inner_outer_txn_pre_adj',
            output_csv=True):
        """
        Process and save inner/outer London weekly transaction data.

        Retrieves data using get_inner_outer_weekly_summary and saves to
        database and/or CSV. Replaces all data in the target table each time.

        Parameters
        ----------
        target_table: str
            Target PostgreSQL table name for storing the processed data
        output_csv: bool
            Whether to also save the data as CSV

        Returns
        -------
        pd.DataFrame
            The processed dataframe with inner/outer London weekly transaction data
        """
        # Get the data (no need to check existing data since we're refreshing completely)
        self.logger.info("Processing all inner/outer weekly data")
        df = self.get_inner_outer_weekly_summary()

        if df.empty:
            self.logger.info("No data to process")
            return df

        self.logger.info(f"Processing {len(df)} rows of inner/outer weekly data")

        # Save to database
        try:
            # Use truncate_and_load_to_postgres to refresh the table
            self.data_writer.truncate_and_load_to_postgres(
                df,
                table_name=target_table,
                schema="gisapdata",
                index=False
            )
            self.logger.info(f"Refreshed table {target_table} with {len(df)} rows")

            # Save to CSV if requested
            if output_csv:
                csv_path = (f"{self.base_dir}mastercard/weekly/"
                            f"processed/inner_outer_weekly_txn_pre_adj.csv")
                df.to_csv(csv_path, index=False)
                self.logger.info(f"Saved data to CSV: {csv_path}")

        except Exception as e:
            self.logger.error(f"Error saving data: {e}")

        return df

    def incremental_refresh_mcard_stg_18_zoom(self):
        """
        Performs an efficient incremental refresh of the existing staging table.
        Only loads new data since the last high watermark.
        """
        self.logger.info(
            "Starting incremental refresh of econ_busyness_mcard_stg_18_zoom")

        try:
            with self.engine.begin() as conn:
                # Step 1: Determine high watermark from existing table - FIXED QUERY
                high_watermark_result = conn.execute(text("""
                    -- Get the true latest data point by ordering by year and week
                    WITH ranked_dates AS (
                        SELECT
                            yr,
                            wk,
                            ROW_NUMBER() OVER (ORDER BY yr DESC, wk DESC) as rn
                        FROM econ_busyness_mcard_stg_18_zoom
                        GROUP BY yr, wk
                    )
                    SELECT yr, wk
                    FROM ranked_dates
                    WHERE rn = 1;
                """)).fetchone()

                if high_watermark_result:
                    max_yr = high_watermark_result[0]
                    max_wk = high_watermark_result[1]
                    self.logger.info(
                        f"Found existing data up to Year {max_yr}, Week {max_wk}")
                else:
                    # If table is empty, set default values
                    max_yr = 2010
                    max_wk = 1
                    self.logger.info(
                        "No existing data found, starting from Year 2010, Week 1")

                # Step 2: Insert only new records directly
                insert_result = conn.execute(text("""
                    INSERT INTO econ_busyness_mcard_stg_18_zoom
                    SELECT DISTINCT ON (
                        yr, wk, industry, segment, geo_name, quad_id, weekday_weekend
                    )
                        yr, wk, industry, segment, geo_name, quad_id, txn_amt,
                        txn_cnt, weekday_weekend
                    FROM econ_busyness_mcard_raw_18_zoom raw
                    WHERE industry IN ('Total Retail', 'Total Apparel', 'Eating Places')
                      AND segment IN ('International', 'Overall')
                      AND geo_name = 'London'
                      AND (raw.yr > :max_yr OR (raw.yr = :max_yr AND raw.wk > :max_wk))
                    ORDER BY yr, wk, industry, segment, geo_name, quad_id,
                        weekday_weekend, txn_amt;
                """), {"max_yr": max_yr, "max_wk": max_wk})

                new_record_count = insert_result.rowcount

                self.logger.info(
                    f"Added {new_record_count} new records to staging table")

                # Step 3: Update statistics if we added records
                if new_record_count > 0:
                    conn.execute(text("""
                        -- Update statistics for query planner
                        ANALYZE econ_busyness_mcard_stg_18_zoom;
                    """))

                return new_record_count

        except Exception as e:
            self.logger.error(f"Error during incremental refresh: {e}")
            # Log the full traceback for debugging
            import traceback
            self.logger.error(traceback.format_exc())
            raise

    def incremental_refresh_mcard_mopac_db(self,
                                           mopac_db_credentials=None,
                                           target_table='econ_busyness_mcard_stg_alt',
                                           schema=None,
                                           batch_size=10000):
        """
        OPTIMIZED: Performs an efficient incremental refresh to mopac database.
        Only loads new data since the last high watermark.
        Maps to different column names: cal_year, cal_week, week_commencing, industry,
        segment, quad_id, txn_amt, txn_count, weekday_weekend.
        No industry filter applied, segment filter: ('International', 'Overall')

        Parameters
        ----------
        mopac_db_credentials : dict
            Dictionary containing alternative database credentials
        target_table : str
            Target table name in the mopac database
        schema : str, optional
            Target schema name in the mopac database
        batch_size : int
            Number of records to process in each batch (default: 10000)
        """
        self.logger.info(
            f"Starting OPTIMIZED incremental refresh of {target_table}"
            f" in mopac database")

        # Use provided credentials or default environment variables for alt DB
        if mopac_db_credentials is None:
            mopac_db_credentials = {
                'database': os.getenv("MOPAC_PG_DATABASE"),
                'username': os.getenv("MOPAC_PG_USER"),
                'password': os.getenv("MOPAC_PG_PASSWORD"),
                'host': os.getenv("MOPAC_PG_HOST"),
                'port': os.getenv("MOPAC_PG_PORT")
            }

        # Create alternative database engine
        mopac_engine = create_engine(
            f"postgresql+psycopg2://{mopac_db_credentials['username']}:"
            f"{mopac_db_credentials['password']}@{mopac_db_credentials['host']}:"
            f"{mopac_db_credentials['port']}/{mopac_db_credentials['database']}"
        )

        # Construct full table name with schema if provided
        full_table_name = f"{schema}.{target_table}" if schema else target_table

        try:
            with mopac_engine.begin() as mopac_conn:
                # Step 1: Determine high watermark from existing table
                high_watermark_result = mopac_conn.execute(text(f"""
                    -- Get the true latest data point by ordering by year and week
                    WITH ranked_dates AS (
                        SELECT
                            cal_year,
                            cal_week,
                            ROW_NUMBER() OVER (ORDER BY cal_year DESC,
                                                        cal_week DESC
                            ) as rn
                        FROM {full_table_name}
                        GROUP BY cal_year, cal_week
                    )
                    SELECT cal_year, cal_week
                    FROM ranked_dates
                    WHERE rn = 1;
                """)).fetchone()

                if high_watermark_result:
                    max_yr = high_watermark_result[0]
                    max_wk = high_watermark_result[1]
                    self.logger.info(
                        f"Found existing data up to Year {max_yr}, Week {max_wk}")
                else:
                    # If table is empty, set default values
                    max_yr = 2010
                    max_wk = 1
                    self.logger.info(
                        "No existing data found, starting from Year 2010, Week 1")

            # OPTIMIZATION 1: Get count first for progress tracking
            with self.engine.begin() as source_conn:
                count_query = text("""
                    SELECT COUNT(DISTINCT (
                    yr, wk, industry, segment, quad_id, weekday_weekend))
                    FROM econ_busyness_mcard_raw_18_zoom
                    WHERE segment IN ('International', 'Overall')
                    AND geo_name = 'London'
                    AND (yr > :max_yr OR (yr = :max_yr AND wk > :max_wk))
                """)
                total_new_records = source_conn.execute(
                    count_query, {"max_yr": max_yr, "max_wk": max_wk}).scalar()

                if total_new_records == 0:
                    self.logger.info("No new data to load")
                    return 0

                self.logger.info(f"Found {total_new_records:,} new records to process")

            # OPTIMIZATION 2: Use streaming cursor + batched processing
            total_inserted = 0
            batch_count = 0

            with self.engine.begin() as source_conn:
                # OPTIMIZATION 3: Improved query with explicit type casting
                source_query = text("""
                    SELECT DISTINCT ON (
                        yr, wk, industry, segment, quad_id, weekday_weekend
                    )
                        CAST(ROUND(yr) AS INTEGER) as cal_year,
                        CAST(ROUND(wk) AS INTEGER) as cal_week,
                        -- Calculate week_commencing based on yr and wk
                        (DATE_TRUNC('week',
                        TO_DATE(CONCAT(CAST(ROUND(yr) AS TEXT), '0104'), 'YYYYMMDD')
                        + INTERVAL '1 day' * (7 * (CAST(ROUND(wk) AS INTEGER) - 1))
                        ))::DATE as week_commencing,
                        COALESCE(TRIM(industry), 'Unknown') as industry,
                        COALESCE(TRIM(segment), 'Unknown') as segment,
                        CAST(quad_id AS BIGINT) as quad_id,
                        COALESCE(CAST(txn_amt AS NUMERIC), 0) as txn_amt,
                        COALESCE(CAST(txn_cnt AS INTEGER), 0) as txn_count,
                        COALESCE(TRIM(weekday_weekend), 'Unknown') as weekday_weekend
                    FROM econ_busyness_mcard_raw_18_zoom
                    WHERE segment IN ('International', 'Overall')
                    AND geo_name = 'London'
                    AND (yr > :max_yr OR (yr = :max_yr AND wk > :max_wk))
                    AND yr IS NOT NULL AND wk IS NOT NULL
                    ORDER BY yr, wk, industry, segment, quad_id,
                        weekday_weekend, txn_amt;
                """)

                # OPTIMIZATION 4: Use server-side cursor for memory efficiency
                result = source_conn.execution_options(stream_results=True).execute(
                    source_query, {"max_yr": max_yr, "max_wk": max_wk}
                )

                # OPTIMIZATION 5: Process in batches with bulk INSERT
                while True:
                    batch = result.fetchmany(batch_size)
                    if not batch:
                        break

                    batch_count += 1
                    batch_size_actual = len(batch)

                    self.logger.info(
                        f"Processing batch {batch_count},"
                        f" size: {batch_size_actual:,}")

                    # Convert to list of dictionaries for bulk insert
                    columns = [
                        'cal_year', 'cal_week', 'week_commencing', 'industry',
                        'segment', 'quad_id', 'txn_amt',
                        'txn_count', 'weekday_weekend']
                    data_dicts = [dict(zip(columns, row)) for row in batch]

                    # OPTIMIZATION 6: Bulk insert with better performance
                    with mopac_engine.begin() as mopac_conn:
                        # Use VALUES clause for better performance than executemany
                        if batch_size_actual > 1:
                            # Multi-row INSERT for better performance
                            values_clause = ','.join([
                                f"({row['cal_year']}, {row['cal_week']}, "
                                f"'{row['week_commencing']}', '{row['industry']}', '{row['segment']}', "  # noqa: E501
                                f"{row['quad_id']}, {row['txn_amt']}, {row['txn_count']}, '{row['weekday_weekend']}')"  # noqa: E501
                                for row in data_dicts
                            ])

                            bulk_insert_stmt = text(f"""
                                INSERT INTO {full_table_name} (
                                    cal_year, cal_week, week_commencing, industry,
                                    segment, quad_id, txn_amt, txn_count, weekday_weekend
                                ) VALUES {values_clause}
                            """)

                            mopac_conn.execute(bulk_insert_stmt)
                        else:
                            # Single row INSERT for small batches
                            insert_stmt = text(f"""
                                INSERT INTO {full_table_name} (
                                    cal_year, cal_week, week_commencing, industry,
                                    segment, quad_id, txn_amt, txn_count, weekday_weekend
                                ) VALUES (
                                    :cal_year, :cal_week, :week_commencing, :industry,
                                    :segment, :quad_id, :txn_amt, :txn_count,
                                    :weekday_weekend
                                )
                            """)
                            mopac_conn.execute(insert_stmt, data_dicts)

                        total_inserted += batch_size_actual

                        # Progress update
                        progress = (
                            total_inserted / total_new_records * 100) if total_new_records > 0 else 0  # noqa: E501
                        self.logger.info(
                            f"✅ Inserted batch {batch_count}: {total_inserted:,}/"
                            f"{total_new_records:,} ({progress:.1f}%)")

            # Step 3: Final statistics update
            with mopac_engine.begin() as mopac_conn:
                if total_inserted > 0:
                    mopac_conn.execute(text(f"""
                        -- Update statistics for query planner
                        ANALYZE {full_table_name};
                    """))

            self.logger.info(f"✅ Completed: Added {total_inserted:,}"
                             f"new records to {full_table_name}")
            return total_inserted

        except Exception as e:
            self.logger.error(f"Error during incremental refresh to mopac DB: {e}")
            # Log the full traceback for debugging
            import traceback
            self.logger.error(traceback.format_exc())
            raise
        finally:
            # Close mopac engine
            mopac_engine.dispose()

    def concat_and_load_all_mcard_weekly_txn_layers(
        self,
        query_file: str = 'weekly_txn_all_layers_concat_query.sql',
        target_table: str = 'econ_busyness_mcard_txn',
        truncate: bool = True,
        load_to_db: bool = True
    ) -> None:
        """
        Concatenate Mastercard weekly transaction data from all layer tables
        (bids, highstreets, towncentres, caz, bespoke, boroughs)
        and load into a combined table using a SQL-based approach for maximum efficiency.

        Args:
            query_file: Name of the SQL query file that performs the concatenation
            target_table: Target table to load the concatenated data into
            truncate: Whether to truncate the target table before loading
            load_to_db: Whether to execute the load operation
        """
        try:
            # Get the concatenation query from weekly directory
            concat_query = self.sql_manager.get_query(f"weekly/{query_file}")

            if load_to_db:
                # Construct the loading query with performance optimizations
                load_query = f"""
                -- Start transaction
                BEGIN;

                -- Maximize performance settings
                SET LOCAL maintenance_work_mem = '2GB';
                SET LOCAL work_mem = '1GB';
                SET LOCAL temp_buffers = '1GB';
                SET LOCAL synchronous_commit = OFF;
                SET LOCAL join_collapse_limit = 8;
                SET LOCAL from_collapse_limit = 8;

                -- Disable autovacuum during load
                ALTER TABLE {target_table} SET (autovacuum_enabled = false);

                -- Truncate if requested
                {f'TRUNCATE TABLE {target_table};' if truncate else ''}

                -- Create unlogged temp table with concatenated data
                CREATE UNLOGGED TABLE temp_concat_weekly_txn AS
                {concat_query};

                -- Create indexes on temp table for faster loading
                CREATE INDEX ON temp_concat_weekly_txn (week_start, yr, wk);
                CREATE INDEX ON temp_concat_weekly_txn (layer, id);

                -- Bulk insert from temp table
                INSERT INTO {target_table}
                SELECT * FROM temp_concat_weekly_txn;

                -- Cleanup
                DROP TABLE temp_concat_weekly_txn;

                -- Reset table settings and analyze
                ALTER TABLE {target_table} SET (autovacuum_enabled = true);
                ANALYZE {target_table};

                -- Commit transaction
                COMMIT;
                """

                with self.engine.connect().execution_options(
                    isolation_level="AUTOCOMMIT"
                ) as connection:
                    start_time = datetime.now()
                    connection.execute(text(load_query))

                    # Log load statistics
                    stats = connection.execute(text(f"""
                        SELECT
                            COUNT(*) as row_count,
                            MIN(week_start)::DATE as min_date,
                            MAX(week_start)::DATE as max_date,
                            COUNT(DISTINCT layer) as layer_count,
                            array_agg(DISTINCT layer) as layers
                        FROM {target_table}
                    """)).fetchone()

                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    rows_per_second = stats.row_count / duration if duration > 0 else 0

                    self.logger.info(
                        f"Successfully combined {stats.layer_count} weekly txn layers"
                        f" into {target_table}:\n"
                        f"Layers: {stats.layers}\n"
                        f"Total rows: {stats.row_count:,}\n"
                        f"Date range: {stats.min_date} to {stats.max_date}\n"
                        f"Duration: {duration:.2f} seconds\n"
                        f"Performance: {rows_per_second:,.0f} rows/second"
                    )

        except Exception as e:
            self.logger.error(f"Error in concat_and_load_all_mcard_weekly_txn_layers:"
                              f" {str(e)}")
            raise

    def concat_and_load_all_mcard_weekly_yoy_layers(
        self,
        query_file: str = 'weekly_yoy_all_layers_concat_query.sql',
        target_table: str = 'econ_busyness_mcard_yoy',
        truncate: bool = True,
        load_to_db: bool = True
    ) -> None:
        """
        Concatenate Mastercard weekly year-over-year data from all layer tables
        (bids, highstreets, towncentres, caz, bespoke, boroughs)
        and load into a combined table using a SQL-based approach for maximum efficiency.

        Args:
            query_file: Name of the SQL query file that performs the concatenation
            target_table: Target table to load the concatenated data into
            truncate: Whether to truncate the target table before loading
            load_to_db: Whether to execute the load operation
        """
        try:
            # Get the concatenation query from weekly directory
            concat_query = self.sql_manager.get_query(f"weekly/{query_file}")

            if load_to_db:
                # Construct the loading query with performance optimizations
                load_query = f"""
                -- Start transaction
                BEGIN;

                -- Maximize performance settings
                SET LOCAL maintenance_work_mem = '2GB';
                SET LOCAL work_mem = '1GB';
                SET LOCAL temp_buffers = '1GB';
                SET LOCAL synchronous_commit = OFF;
                SET LOCAL join_collapse_limit = 8;
                SET LOCAL from_collapse_limit = 8;

                -- Disable autovacuum during load
                ALTER TABLE {target_table} SET (autovacuum_enabled = false);

                -- Truncate if requested
                {f'TRUNCATE TABLE {target_table};' if truncate else ''}

                -- Create unlogged temp table with concatenated data
                CREATE UNLOGGED TABLE temp_concat_weekly_yoy AS
                {concat_query};

                -- Create indexes on temp table for faster loading
                CREATE INDEX ON temp_concat_weekly_yoy (week_start, yr, wk);
                CREATE INDEX ON temp_concat_weekly_yoy (layer, id);

                -- Bulk insert from temp table
                INSERT INTO {target_table}
                SELECT * FROM temp_concat_weekly_yoy;

                -- Cleanup
                DROP TABLE temp_concat_weekly_yoy;

                -- Reset table settings and analyze
                ALTER TABLE {target_table} SET (autovacuum_enabled = true);
                ANALYZE {target_table};

                -- Commit transaction
                COMMIT;
                """

                with self.engine.connect().execution_options(
                    isolation_level="AUTOCOMMIT"
                ) as connection:
                    start_time = datetime.now()
                    connection.execute(text(load_query))

                    # Log load statistics
                    stats = connection.execute(text(f"""
                        SELECT
                            COUNT(*) as row_count,
                            MIN(week_start)::DATE as min_date,
                            MAX(week_start)::DATE as max_date,
                            COUNT(DISTINCT layer) as layer_count,
                            array_agg(DISTINCT layer) as layers
                        FROM {target_table}
                    """)).fetchone()

                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    rows_per_second = stats.row_count / duration if duration > 0 else 0

                    self.logger.info(
                        f"Successfully combined {stats.layer_count} weekly yoy layers"
                        f" into {target_table}:\n"
                        f"Layers: {stats.layers}\n"
                        f"Total rows: {stats.row_count:,}\n"
                        f"Date range: {stats.min_date} to {stats.max_date}\n"
                        f"Duration: {duration:.2f} seconds\n"
                        f"Performance: {rows_per_second:,.0f} rows/second"
                    )

        except Exception as e:
            self.logger.error(f"Error in concat_and_load_all_mcard_weekly_yoy_layers:"
                              f" {str(e)}")
            raise
