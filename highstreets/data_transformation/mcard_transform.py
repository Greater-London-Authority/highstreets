import logging
import re

import numpy as np
import pandas as pd
from sqlalchemy.sql import text
from datetime import datetime

from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.api.clientbase import APIClient
from highstreets.core.sql_manager import SQLManager


class McardTransform:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler())
        self.base_dir = config.BASE_DIR
        self.sectors_df = config.SECTORS_DF
        self.adjustment_factor_dir = config.ADJUSTMENT_FACTOR_DIR
        self.inner_outer_quad_dir = config.INNER_OUTER_QUAD_DIR
        # Add SQL manager
        self.sql_manager = SQLManager()
        self.data_loader = DataLoader()
        # Get database engine from DataLoader
        self.engine = self.data_loader.engine

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
            f"{self.base_dir}reference_data/mcard_grid_to_ldn_ref_lookup.csv"
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

    # INFLATION ADJUSTMENT
    def inflation_adjust(
        self,
        spend,
        cpi_table,
        reindexing_year=None,
        col_to_adjust=["txn_amt"],
        date_col="count_date",
    ):
        """
        Adjusts spend columns 'txn_amt' and 'avg_spend_amt by
        monthly ONS inflation rates.
        spend: spend table of MC 3-hourly/weekly/Spending Pulse data
        cpi_table: imported and cleaned CPIH table from ONS
        reindexing year (optional): the year that you want to use as cpi_index = 100.
        If none, does not reindex beyond ONS's existing 2015=100 reindex
        """
        # Reindex to a chosen baseline year, otherwise skip
        if reindexing_year is not None:
            reindex = cpi_table[cpi_table["yr"] == reindexing_year]["cpi_index"].mean()
            cpi_table["cpi_index"] = cpi_table["cpi_index"] / reindex * 100
        else:
            pass
        # to match with new table
        cpi_table["cpi_index"] = cpi_table["cpi_index"].round(4)

        # Add month and yr column to spend data
        spend["month"] = spend[date_col].dt.month
        spend["yr"] = spend[date_col].dt.year

        # Join spend data with cpi data
        spend = pd.merge(spend, cpi_table, how="left", on=["yr", "month"])

        # If spend data is more recent than cpi data, there will be NaNs.
        # Fill them with the latest available cpi index
        max_year = cpi_table["yr"].max()
        max_month = cpi_table[(cpi_table["yr"] == max_year)]["month"].max()
        spend["cpi_index"].fillna(
            cpi_table[
                (cpi_table["month"] == max_month) & (cpi_table["yr"] == max_year)
            ]["cpi_index"]
        )

        # Adjust
        for col in col_to_adjust:
            spend[f"{col}"] = spend[col] / spend["cpi_index"] * 100
        spend.drop(columns=["aggregate", "cpi_index"], inplace=True)
        return spend

    def mcard_adjust(
        self,
        spend,
        col_to_adjust="txn_amt",
        adj_col="adjustment_factor_retail",
        date_col="count_date",
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
        sectors_df: pre-defined dataframe linking GeoInsights, Spending Pulse and CPI
        sectors

        Returns
        --------
        Dataframe with additional adjusted spend column (e.g. txn_amt_adj)
        """
        data_loader = DataLoader()
        # adjustment_factor = data_loader.get_full_data(
        # "econ_busyness_mcard_adjustment_factor")
        inner_outer_quad = data_loader.get_full_data(
            "econ_busyness_mcard_Inner_Outer_quad_lookup"
        )
        inner_outer_quad["quad_id"] = inner_outer_quad["quad_id"].astype("Int64")
        adjustment_factor = pd.read_csv(self.adjustment_factor_dir)
        # inner_outer_quad = pd.read_csv(self.inner_outer_quad_dir)
        # where a quad is assigned both Inner and Outer - keep Outer
        inner_outer_quad = inner_outer_quad.sort_values(
            by="inner_outer"
        ).drop_duplicates(subset="quad_id", keep="last")
        # import ONS's CPIH table via API
        api_client = APIClient()
        cpi_table = api_client.fetch_cpi()

        # Add month and yr column to spend data
        spend[date_col] = pd.to_datetime(spend[date_col])
        spend["month"] = spend[date_col].dt.month
        spend["yr"] = spend[date_col].dt.year

        # Add inner_outer to spend data
        spend = pd.merge(spend, inner_outer_quad, how="left", on="quad_id")

        # Join spend data with mcard adjustment data
        # # need to merge on inner vs outer too
        spend = pd.merge(
            spend,
            adjustment_factor[["yr", "month", "inner_outer", adj_col]],
            how="left",
            on=["yr", "month", "inner_outer"],
        )

        # If spend data is more recent than Spending Pulse, there will be NaNs.
        # Fill them with the latest available mcard_adjustment
        spend = spend.sort_values(by=["inner_outer", date_col])
        spend[adj_col] = spend[adj_col].fillna(method="ffill")

        # Adjust for cash-to-card shift and MC market share - create an additional column
        spend[col_to_adjust + "_adj"] = spend[col_to_adjust] / spend[adj_col]

        # remove unecessary columns
        spend.drop(columns=["inner_outer", adj_col], inplace=True)

        # Adjust for inflation (using subcategory-specific CPI)
        txn_cat_cpi_dict = (
            self.sectors_df[["geo_insights", "cpi"]]
            .set_index("geo_insights")
            .T.to_dict("records")[0]
        )
        if col_to_adjust == "txn_amt":
            txn_cat = "retail"
        else:
            # eating / apparel / retail
            txn_cat = col_to_adjust.split("_")[-1]
        # adjust for inflation (subcat specific CPI) - updates adjusted column
        spend = self.inflation_adjust(
            spend,
            cpi_table[cpi_table["Aggregate"] == txn_cat_cpi_dict[txn_cat]][
                ["yr", "month", "Aggregate", "cpi_index"]
            ],
            reindexing_year=2018,
            col_to_adjust=[col_to_adjust + "_adj"],
            date_col=date_col,
        )

        return spend

    def mcard_highstreet_threehourly_transform(self, data):
        data_loader = DataLoader()
        Highstreets_quad_lookup = data_loader.get_full_data(
            "econ_busyness_mcard_Highstreets_quad_lookup"
        )
        Highstreets_quad_lookup["quad_id"] = Highstreets_quad_lookup["quad_id"].astype(
            "Int64"
        )
        data["quad_id"] = data["quad_id"].astype("Int64")
        # Highstreets_quad_lookup = pd.read_csv(
        #     f"{self.base_dir}reference_data/Highstreets_quad_lookup.csv"
        # )
        data = (
            Highstreets_quad_lookup.merge(
                data, left_on="quad_id", right_on="quad_id", how="right"
            )
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
            .aggregate(
                txn_amt=("txn_amt", lambda x: round(x.sum(), 2)),
                txn_amt_adj=("txn_amt_adj", lambda x: round(x.sum(), 2)),
                txn_cnt=("txn_cnt", lambda x: round(x.sum(), 2)),
            )
            .reset_index()
        )
        data["highstreet_id"] = data["highstreet_id"].astype(int)

        return data

    def mcard_towncentre_threehourly_transform(self, data):
        data_loader = DataLoader()
        TownCentres_quad_lookup = data_loader.get_full_data(
            "econ_busyness_mcard_TownCentres_quad_lookup"
        )
        TownCentres_quad_lookup["quad_id"] = TownCentres_quad_lookup["quad_id"].astype(
            "Int64"
        )
        data["quad_id"] = data["quad_id"].astype("Int64")
        # TownCentres_quad_lookup = pd.read_csv(
        #     f"{self.base_dir}reference_data/TownCentres_quad_lookup.csv"
        # )
        data = (
            TownCentres_quad_lookup.merge(
                data, left_on="quad_id", right_on="quad_id", how="right"
            )
            .dropna(subset=["tc_id"])
            .groupby(["tc_id", "tc_name", "count_date", "hours", "borough", "x", "y"])
            .aggregate(
                txn_amt=("txn_amt", lambda x: round(x.sum(), 2)),
                txn_amt_adj=("txn_amt_adj", lambda x: round(x.sum(), 2)),
                txn_cnt=("txn_cnt", lambda x: round(x.sum(), 2)),
            )
            .reset_index()
        )
        data["tc_id"] = data["tc_id"].astype(int)

        return data

    def mcard_bid_threehourly_transform(self, data):
        data_loader = DataLoader()
        BIDS_quad_lookup = data_loader.get_full_data(
            "econ_busyness_mcard_BIDs_quad_lookup"
        )
        BIDS_quad_lookup["quad_id"] = BIDS_quad_lookup["quad_id"].astype("Int64")
        data["quad_id"] = data["quad_id"].astype("Int64")
        data = (
            BIDS_quad_lookup.merge(
                data, left_on="quad_id", right_on="quad_id", how="right"
            )
            .dropna(subset=["bid_id"])
            .groupby(["bid_id", "bid_name", "count_date", "hours"])
            .aggregate(
                txn_amt=("txn_amt", lambda x: round(x.sum(), 2)),
                txn_amt_adj=("txn_amt_adj", lambda x: round(x.sum(), 2)),
                txn_cnt=("txn_cnt", lambda x: round(x.sum(), 2)),
            )
            .reset_index()
        )
        data["bid_id"] = data["bid_id"].astype(int)

        return data

    def mcard_bespoke_threehourly_transform(self, data):
        data_loader = DataLoader()
        bespoke_quad_lookup = data_loader.get_full_data(
            "econ_busyness_mcard_bespoke_quad_lookup"
        )
        bespoke_quad_lookup["quad_id"] = bespoke_quad_lookup["quad_id"].astype("Int64")
        data["quad_id"] = data["quad_id"].astype("Int64")
        # bespoke_quad_lookup = pd.read_csv(
        #     f"{self.base_dir}reference_data/bespoke_quad_lookup.csv"
        # )
        data = (
            bespoke_quad_lookup.merge(
                data, left_on="quad_id", right_on="quad_id", how="right"
            )
            .dropna(subset=["bespoke_area_id"])
            .groupby(["bespoke_area_id", "name", "count_date", "hours"])
            .aggregate(
                txn_amt=("txn_amt", lambda x: round(x.sum(), 2)),
                txn_amt_adj=("txn_amt_adj", lambda x: round(x.sum(), 2)),
                txn_cnt=("txn_cnt", lambda x: round(x.sum(), 2)),
            )
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

    def fetch_and_transform_mcard_data(
        self,
        transform_layer: str,
        table_name: str,
        truncate: bool = False,
        load_to_db: bool = True,
    ) -> None:
        """
        Transform and load Mastercard data using highly optimized bulk insert.

        Args:
            transform_layer: Name of the transformation query file
            table_name: Target table name to load data into
            truncate: Whether to truncate the target table first
            load_to_db: Whether to execute the load
        """
        try:
            # Get transformation query and ensure it doesn't end with semicolon
            transform_query = self.sql_manager.get_query(
                transform_layer, "mcard/threehourly"
            )

            # Remove any trailing semicolons that might cause syntax errors
            transform_query = transform_query.strip()
            if transform_query.endswith(";"):
                transform_query = transform_query[:-1]

            # Check if we have a valid query
            if not transform_query or len(transform_query.strip()) < 10:
                raise ValueError(
                    f"Transform query is empty or" f" too short: '{transform_query}'"
                )

            self.logger.info(
                f"Running transformation:" f" {transform_layer} for table: {table_name}"
            )

            if load_to_db:
                # Construct the loading query with maximum performance optimizations
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
                ALTER TABLE {table_name} SET (autovacuum_enabled = false);

                -- Truncate if requested
                {f'TRUNCATE TABLE {table_name};' if truncate else ''}

                -- Create unlogged temp table with transformed data
                CREATE UNLOGGED TABLE temp_transformed AS
                {transform_query};

                -- Create index on temp table for faster joining
                CREATE INDEX ON temp_transformed (count_date, hours);

                -- Bulk insert from temp table
                INSERT INTO {table_name}
                SELECT * FROM temp_transformed;

                -- Cleanup
                DROP TABLE temp_transformed;

                -- Reset table settings and analyze
                ALTER TABLE {table_name} SET (autovacuum_enabled = true);
                ANALYZE {table_name};

                -- Commit transaction
                COMMIT;
                """

                with self.engine.connect().execution_options(
                    isolation_level="AUTOCOMMIT"
                ) as connection:
                    start_time = datetime.now()
                    connection.execute(text(load_query))

                    # Log load statistics
                    stats = connection.execute(
                        text(
                            f"""
                        SELECT
                            COUNT(*) as row_count,
                            MIN(count_date)::DATE as min_date,
                            MAX(count_date)::DATE as max_date
                        FROM {table_name}
                    """
                        )
                    ).fetchone()

                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    rows_per_second = stats.row_count / duration if duration > 0 else 0

                    self.logger.info(
                        f"Successfully loaded {stats.row_count:,}"
                        f" rows into {table_name}\n"
                        f"Date range: {stats.min_date} to {stats.max_date}\n"
                        f"Duration: {duration:.2f} seconds\n"
                        f"Performance: {rows_per_second:,.0f} rows/second"
                    )

        except Exception as e:
            self.logger.error(f"Error in fetch_and_transform_mcard_data: {str(e)}")
            raise


def adjust_mcard_data_sql(
    self,
    query="update_mcard_adjustment_no_merge.sql",
    table_name="econ_busyness_mrli_3hourly",
) -> bool:
    """
    Execute SQL-based Mastercard adjustment with optimized performance.

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the optimized SQL query
        sql_file = query
        adjustment_sql = self.sql_manager.get_query(sql_file)

        # Log the start of adjustment process
        self.logger.info("Starting optimized SQL-based Mastercard adjustment")

        # Get row count to be adjusted (for progress tracking)
        with self.engine.connect() as conn:
            row_count = conn.execute(
                text(f"SELECT COUNT(*) FROM {table_name}")
            ).scalar()

        start_time = datetime.now()
        self.logger.info(f"Adjusting {row_count:,} rows...")

        # Execute the SQL with high performance settings
        with self.engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            # Set database configuration for better performance
            conn.execute(text("SET statement_timeout = 0"))  # No timeout
            conn.execute(text("SET work_mem = '1GB'"))
            # More memory for sorting/joins

            # Execute the actual adjustment SQL
            conn.execute(text(adjustment_sql))

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        rows_per_second = row_count / duration if duration > 0 else 0

        # Log performance statistics
        self.logger.info(
            f"Successfully adjusted {row_count:,} rows\n"
            f"Duration: {duration:.2f} seconds\n"
            f"Performance: {rows_per_second:,.0f} rows/second"
        )

        return True

    except Exception as e:
        self.logger.error(f"Error in adjust_mcard_data_sql: {str(e)}")
        return False
