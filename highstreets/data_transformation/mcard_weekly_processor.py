# file_processor.py
import os
import re
import pandas as pd
import numpy as np
import geopandas as gpd
from datetime import datetime
from highstreets import config
from highstreets.api.clientbase import APIClient
from sqlalchemy import create_engine, text
from highstreets.data_transformation.mcard_transform import McardTransform
from glob import glob
import logging


class FileProcessor:
    def __init__(self, data_loader, data_writer, dir_path: str):
        self.data_loader = data_loader
        self.data_writer = data_writer
        self.sectors_df = config.SECTORS_DF
        self.base_dir = config.BASE_DIR
        self.adjustment_factor_dir = config.ADJUSTMENT_FACTOR_DIR
        self.inner_outer_quad_dir = config.INNER_OUTER_QUAD_DIR
        self.dir_path = dir_path
        self.new_files = []
        self.existing_files = []
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

    def calculate_yoy_growth_compared_to_2019(self, df, col, new_col, ids='inner_outer'):
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
                df.sort_values(by=[ids, "yr", "wk"], inplace=True)
                # df[new_col] = None
                df_2019 = df[df['yr'].isin([2018, 2019])]
                df_2019[new_col] = df_2019[col].div(
                    df_2019.groupby([ids, "wk"])[col].transform('first'))

                df_2019_onwards = df[df['yr'] != 2018]
                df_2019_onwards[new_col] = df_2019_onwards[col].div(
                    df_2019_onwards.groupby([ids, "wk"])[col].transform('first'))
                df = pd.concat([df_2019, df_2019_onwards[df_2019_onwards['yr'] != 2019]],
                               axis=0)
                df.sort_values(by=[ids, "yr", "wk"], inplace=True)

                # Set YOY growth to NaN for the first entry of each 'id' and 'wk'
                df.loc[df.groupby([ids, "wk"]).head(1).index, new_col] = None
                # Handle division by zero by replacing resulting infinite values with NaN
                df[new_col].replace([np.inf, -np.inf], np.nan, inplace=True)
                # Sort the DataFrame by 'id', 'yr', and 'wk' to arrange years
                #  in ascending order
                df.sort_values(by=["yr", "wk", ids], inplace=True)
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
    def mcard_adjust_weekly(self, spend, lookup_file, quad_lookup_file,
                            col_to_adjust=['txn_amt'],
                            date_col='count_date',
                            poi_id='quad_id',
                            filename='txn'):
        '''
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
        '''

        # Add month and yr column to spend data
        spend[date_col] = pd.to_datetime(spend[date_col])
        spend['month'] = spend[date_col].dt.month
        spend['yr'] = spend[date_col].dt.year

        if lookup_file is not None:
            # Merge quad lookup to spend
            quad_lookup = quad_lookup_file

            # Load quad inner_outer lookup
            inner_outer_quad = lookup_file
            # where a quad is assigned both Inner and Outer - keep Outer
            inner_outer_quad = inner_outer_quad.sort_values(
                by='inner_outer').drop_duplicates(subset='quad_id', keep='last')
            # Add inner_outer to spend data
            inner_outer_poi = pd.merge(quad_lookup[['quad_id', poi_id]],
                                       inner_outer_quad, how='left', on='quad_id')

            # Designate Inner or Outer to each POI - take most common POI
            poi_io_lookup = inner_outer_poi.groupby(poi_id)[
                'inner_outer'].agg(lambda x : x.mode()[0]).reset_index()

            # Add IO to poi lookup to spend data
            spend = pd.merge(spend, poi_io_lookup, how='left', on=poi_id)

        # Join spend data with mcard adjustment data
        adjustment_factor = pd.read_csv(self.adjustment_factor_dir)

        # need to merge on inner vs outer too
        spend = pd.merge(spend, adjustment_factor, how='left',
                         left_on=['yr', 'month', 'inner_outer'],
                         right_on=['yr', 'month', 'inner_outer'])

        # If spend data is more recent than Spending Pulse, there will be NaNs.
        # Fill them with the latest available mcard_adjustment
        spend = spend.sort_values(by=['inner_outer', date_col])
        for adj_col in [i for i in spend.columns if i.startswith('adjustment_factor_')]:
            spend[adj_col] = spend[adj_col].fillna(method='ffill')

        # Adjust for cash-to-card shift and MC market share - create an additional column
        for col in col_to_adjust:
            if col == 'txn_amt':
                spend[col + '_adj'] = spend[col] / spend['adjustment_factor_retail']
            else:
                spend[col + '_adj'] = spend[col] / spend[
                    f'adjustment_factor_{col.split("_")[-1]}']

        if lookup_file is not None:
            spend.drop(columns=['inner_outer'] + [
                i for i in spend.columns if i.startswith('adjustment_factor_')],
                inplace=True)  # remove unecessary columns
        else:
            spend.drop(columns=[
                i for i in spend.columns if i.startswith('adjustment_factor_')],
                inplace=True)  # remove unecessary columns

        # Adjust for inflation (using subcategory-specific CPI)
        # import ONS's CPIH table via API
        api_client = APIClient()
        cpi_table = api_client.fetch_cpi()
        txn_cat_cpi_dict = self.sectors_df[
            ['geo_insights', 'cpi']].set_index('geo_insights').T.to_dict('records')[0]

        for col in col_to_adjust:
            if col == 'txn_amt':
                txn_cat = 'retail'
            else:
                txn_cat = col.split('_')[-1]  # eating / apparel / retail
            # using the same inflation adjustment method used for mcard threehourly
            mcard_transform = McardTransform()
            # adjust for inflation (subcat specific CPI) - updates adjusted column
            spend = mcard_transform.inflation_adjust(spend, cpi_table[
                cpi_table['Aggregate'] == txn_cat_cpi_dict[txn_cat]][
                    ['yr', 'month', 'Aggregate', 'cpi_index']],
                reindexing_year=2018, col_to_adjust=[col + '_adj'], date_col=date_col)

            spend[col + '_adj'] = spend[col + '_adj'].round(3)

        spend['yr'] = spend[date_col].dt.isocalendar().year
        spend.drop(columns=['month'], inplace=True)  # remove unecessary columns

        spend.to_csv(f"{self.base_dir}mastercard/weekly/processed/{filename}.csv",
                     index=False)
        # Adding in a function to calculate YoY
        yoy = spend.copy()

        for col in [i for i in yoy.columns if i.startswith('txn_')]:
            yoy = self.calculate_yoy_growth_compared_to_2019(yoy, col,
                                                             f'yoy_{col}',
                                                             ids=poi_id)

        # -----------------------------------------------------------------------------
        # Additional code to get YOY file in correct format - before saving to CSV
        # -----------------------------------------------------------------------------

        # Filter for 2019 onwards
        yoy = yoy[yoy['yr'] >= 2019]

        # For HS / TC  areas add in centroid x,y and borough columns

        if poi_id == 'highstreet_name':
            # Get borough for each area

            query = "select * from econ_busyness_mcard_Highstreets_quad_lookup"

            hs_bor_lookup = pd.read_sql(
                text(query), self.engine.connect())

            hs_bor_lookup = hs_bor_lookup[
                ['highstreet_id',
                 'highstreet_name',
                 'x',
                 'y',
                 'borough']].drop_duplicates()
            hs_bor_lookup['highstreet_id'] = hs_bor_lookup[
                'highstreet_id'].astype('int64')

            yoy['highstreet_id'] = yoy['highstreet_id'].astype('Int64')

            yoy = pd.merge(
                yoy, hs_bor_lookup, on=['highstreet_id', 'highstreet_name'], how='left')

        elif poi_id == 'tc_name':

            query = "select * from econ_busyness_mcard_TownCentres_quad_lookup"

            tc_bor_lookup = pd.read_sql(
                text(query), self.engine.connect()
            )

            tc_bor_lookup = tc_bor_lookup[
                ['tc_id', 'tc_name', 'x', 'y', 'borough']].drop_duplicates()
            tc_bor_lookup['tc_id'] = tc_bor_lookup['tc_id'].astype('int64')

            yoy['tc_id'] = yoy['tc_id'].astype('Int64')

            yoy = pd.merge(yoy, tc_bor_lookup, on=['tc_id', 'tc_name'], how='left')

        # Get all columns before txn_amt ones
        id_cols = list(yoy.loc[:, :'txn_amt_wd_eating'].columns[:-1])

        # Drop txn_ cols
        yoy = yoy.drop(columns=[i for i in yoy.columns if i.startswith('txn_')]).round(3)

        # Put columns in correct format/order

        if poi_id == 'highstreet_name' or poi_id == 'tc_name':

            yoy = yoy[id_cols + ['x', 'y', 'borough'] + [
                'yoy_txn_amt_wd_eating', 'yoy_txn_amt_we_eating',
                'yoy_txn_amt_wd_apparel', 'yoy_txn_amt_we_apparel',
                'yoy_txn_amt_wd_retail', 'yoy_txn_amt_we_retail',
                'yoy_txn_cnt_wd_eating', 'yoy_txn_cnt_we_eating',
                'yoy_txn_cnt_wd_apparel', 'yoy_txn_cnt_we_apparel',
                'yoy_txn_cnt_wd_retail', 'yoy_txn_cnt_we_retail',
                'yoy_txn_amt_wd_eating_adj', 'yoy_txn_amt_we_eating_adj',
                'yoy_txn_amt_wd_apparel_adj', 'yoy_txn_amt_we_apparel_adj',
                'yoy_txn_amt_wd_retail_adj', 'yoy_txn_amt_we_retail_adj']]

        else:
            yoy = yoy[id_cols + [
                'yoy_txn_amt_wd_eating', 'yoy_txn_amt_we_eating',
                'yoy_txn_amt_wd_apparel', 'yoy_txn_amt_we_apparel',
                'yoy_txn_amt_wd_retail', 'yoy_txn_amt_we_retail',
                'yoy_txn_cnt_wd_eating', 'yoy_txn_cnt_we_eating',
                'yoy_txn_cnt_wd_apparel', 'yoy_txn_cnt_we_apparel',
                'yoy_txn_cnt_wd_retail', 'yoy_txn_cnt_we_retail',
                'yoy_txn_amt_wd_eating_adj', 'yoy_txn_amt_we_eating_adj',
                'yoy_txn_amt_wd_apparel_adj', 'yoy_txn_amt_we_apparel_adj',
                'yoy_txn_amt_wd_retail_adj', 'yoy_txn_amt_we_retail_adj']]
        # -----------------------------------------------------------------------------------------

        yoy.to_csv(f"{self.base_dir}mastercard/weekly/processed"
                   f"/yoy{filename.split('txn')[1]}.csv",
                   index=False)

        return spend
