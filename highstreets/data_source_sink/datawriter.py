import logging
import os
import boto3
import fsspec
import io
import geopandas as gpd
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from glapy import lds
from glapy.database.utils import fast_write
from sqlalchemy import create_engine, text

from highstreets import config

load_dotenv(find_dotenv())

# Suppress botocore checksum validation logs
logging.getLogger("botocore.httpchecksum").setLevel(logging.ERROR)


class DataWriter:
    def __init__(self):
        self.database = os.getenv("PG_DATABASE")
        self.username = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASSWORD")
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT")
        self.lds_api_key = os.getenv("LDS_API_KEY")
        self.base_dir = config.BASE_DIR
        self.base_path = f"{self.base_dir}"
        self.s3_bucket = config.S3_BUCKET
        self.s3_client = boto3.client("s3")
        self.fs = self._get_filesystem(config.BASE_DIR)
        # Create a database connection
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
        self.hs_file_path = {
            "mastercard_3hourly": f"{self.base_path}mastercard/mrli_3hourly/processed/",
            "bt": f"{self.base_path}bt/processed/",
        }

    def _get_filesystem(self, directory):
        if directory.startswith('s3://'):
            return fsspec.filesystem('s3')
        else:
            return fsspec.filesystem('file')

    def load_data_to_csv(self, data, file_path):
        try:
            data.to_csv(file_path, index=False)
            logging.info(f"Data successfully loaded to CSV: {file_path}")
        except Exception as e:
            logging.error(f"Error while loading data to CSV: {e}")

    def write_threehourly_hs_to_s3(self, data, data_source):
        """
        Writes a DataFrame to an S3 bucket as a CSV file based on
        the first column's name and data type.

        Args:
            data (pandas.DataFrame): The DataFrame to be exported.
            data_source (str): The type of data ("mastercard_3hourly" or "bt").

        Raises:
            ValueError: If the data source is invalid.
            Exception: For any other unexpected errors during the export process.
        """
        try:
            if data_source in self.hs_file_path:
                first_column_name = data.columns[0]
                if first_column_name in [
                    "highstreet_id",
                    "tc_id",
                    "bespoke_area_id",
                    "bid_id",
                    "msoa_id",
                    "lsoa_id",
                ]:
                    directory_name = {
                        "highstreet_id": "highstreet",
                        "tc_id": "towncentre",
                        "bespoke_area_id": "bespoke",
                        "bid_id": "bid",
                        "msoa_id": "msoa",
                        "lsoa_id": "lsoa",
                    }[first_column_name]
                    start_date = data["count_date"].min().strftime("%Y-%m-%d")
                    end_date = data["count_date"].max().strftime("%Y-%m-%d")

                    if first_column_name not in ["msoa_id", "lsoa_id"]:
                        data["hours"] = "'" + data["hours"]
                        modify_in_place = True
                    else:
                        modify_in_place = False

                    # Determine filename based on data source and first column
                    if first_column_name in ["msoa_id", "lsoa_id"]:
                        filename = (f"{directory_name}_hourly_"
                                    f"counts_{start_date}_{end_date}.csv")
                    else:
                        if data_source == "mastercard_3hourly":
                            filename = (f"{directory_name}_3hourly"
                                        f"_txn_{start_date}_{end_date}.csv")
                        else:
                            filename = (f"{directory_name}_3hourly"
                                        f"_counts_{start_date}_{end_date}.csv")

                    # Construct the S3 key (path within the bucket)
                    s3_key = (f"{self.hs_file_path[data_source]}"
                              f"{directory_name}/{filename}")
                    s3_key = s3_key.replace("\\", "/")  # Ensure forward slashes
                    print(s3_key)

                    # Debug logging to confirm paths
                    logging.debug(f"S3 bucket: {self.s3_bucket}")
                    logging.debug(f"S3 directory path: {self.hs_file_path[data_source]}")
                    logging.debug(f"S3 subdirectory (directory_name): {directory_name}")
                    logging.debug(f"S3 key (final path): {s3_key}")

                    # Convert DataFrame to CSV and upload to S3
                    with io.StringIO() as csv_buffer:
                        data.to_csv(csv_buffer, index=False)
                        self.s3_client.put_object(
                            Bucket=self.s3_bucket,
                            Key=s3_key,
                            Body=csv_buffer.getvalue()
                        )

                    if modify_in_place:
                        data["hours"] = data["hours"].str.strip("'")

                    logging.info(f"Data successfully written to S3: {s3_key}")
                else:
                    logging.error(f"Invalid column name: {first_column_name}")
            else:
                raise ValueError("Invalid data_source. Supported values"
                                 " are 'mastercard_3hourly' and 'bt'.")
        except self.s3_client.exceptions.NoSuchBucket as e:
            logging.error(f"S3 Bucket not found: {e}")
        except self.s3_client.exceptions.ClientError as e:
            logging.error(f"S3 Client error: {e}")
        except Exception as e:
            logging.error(f"Error while uploading data to S3: {e}")

    def table_exists(self, table_name):
        return self.engine.dialect.has_table(self.engine.connect(), table_name)

    def append_data_to_postgres(self, data, table_name, date_column="count_date"):
        # Check if the table exists in the database
        if self.table_exists(table_name):
            try:
                # Get the max date in the table
                max_date = pd.read_sql_query(
                    text(f"SELECT MAX({date_column}) FROM {table_name}"),  # noqa: S608
                    self.engine.connect(),
                )["max"][0]

                max_date = pd.to_datetime(max_date)
                # Convert date column to Date object
                data[date_column] = pd.to_datetime(data[date_column])

                # Filter the DataFrame to include only rows after the max date
                df_to_append = data[data[date_column] > max_date]

                # Check if there are rows to append
                if len(df_to_append) > 0:
                    # Write the filtered data to the existing table
                    df_to_append.to_sql(
                        name=table_name,
                        con=self.engine,
                        if_exists="append",
                        index=False,
                        schema="gisapdata",
                    )
                    print("Data appended successfully.")
                    logging.info("Data successfully loaded to PostgreSQL")
                else:
                    print("No new data to append.")
            except Exception as e:
                print("Error occurred while appending data:", str(e))
                logging.error(f"Error while loading data to PostgreSQL: {e}")
        else:
            logging.info("The table does not exist")

        # Disconnect from the database
        try:
            self.engine.dispose()
            print("Disconnected from the database.")
        except Exception as e:
            print("Error occurred while disconnecting from the database:", str(e))

    def append_chunk(self, chunk, table_name: str):
        chunk.to_sql(table_name, self.engine, if_exists='append', index=False)
        logging.info(f"Appended chunk to table {table_name}.")

    def append_data_without_check(self, data, table_name):
        # Check if there are rows to append
        if len(data) > 0:
            # Write the filtered data to the existing table
            data.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="append",
                index=False,
                schema="gisapdata",
            )
            print("New Data appended successfully.")
            logging.info("Data successfully loaded to PostgreSQL")
        else:
            logging.info("No new data to append")

    def append_data_with_id_check(self, data, ids, id_col, table_name):
        if self.table_exists(table_name):
            try:
                if ids.size:
                    existing_ids_query = text(
                        f"SELECT DISTINCT {id_col} FROM {table_name} "  # noqa: S608
                        f"WHERE {id_col} IN ({', '.join(map(str, ids))})"
                    )
                    existing_ids = (
                        self.engine.connect().execute(existing_ids_query).fetchall()
                    )
                    if existing_ids:
                        for row in existing_ids:
                            logging.error("IDs already exist in the table: ", row[0])
                            print("Error: IDs already exist in the table.")
                        return False
                    else:
                        logging.info("Appending data to postgres")
                        self.append_data_without_check(data, table_name)
                        return True
            except Exception as e:
                error_msg = f"Error occurred while checking IDs: {str(e)}"
                logging.error(error_msg)

        else:
            logging.info("The table does not exist")

    def load_hsds_lookup_to_postgres(self):
        # bid
        query = (
            "select bid_id, bid_name, geom from "
            "regen_business_improvement_districts_27700_live"
        )
        bid = gpd.GeoDataFrame.from_postgis(
            text(query), self.engine.connect(), geom_col="geom"
        )

        # highstreet
        query = (
            "select highstreet_id, highstreet_name, geom "
            "from regen_high_streets_proposed_2"
        )
        highstreet = gpd.GeoDataFrame.from_postgis(
            text(query), self.engine.connect(), geom_col="geom"
        )

        # towncentre
        query = "select tc_id, tc_name, geom from planning_town_centre_all_2020"
        tc = gpd.GeoDataFrame.from_postgis(
            text(query), self.engine.connect(), geom_col="geom"
        )

        # Add a 'layer' column to each table to differentiate the data
        bid["layer"] = "bids"
        highstreet["layer"] = "highstreets"
        tc["layer"] = "towncentres"

        highstreet = highstreet.rename(
            columns={"highstreet_id": "id", "highstreet_name": "name"}
        )
        tc = tc.rename(columns={"tc_id": "id", "tc_name": "name"})
        bid = bid.rename(columns={"bid_id": "id", "bid_name": "name"})

        # Select the desired columns from each table
        bid = bid[["id", "name", "layer", "geom"]]
        highstreet = highstreet[["id", "name", "layer", "geom"]]
        tc = tc[["id", "name", "layer", "geom"]]

        # Merge the dataframes into a single dataframe
        frames = [highstreet, tc, bid]
        merged_df = pd.concat(frames)
        merged_df.info()
        merged_df = merged_df.replace("\n", "", regex=True)

        fast_write(merged_df, "hsds_bid_hs_tc", if_exists="truncate")

    def write_hex_to_csv_by_year(self, data, output_dir, custom_file_name=None):
        """
        Writes DataFrame to CSV files by year.

        Args:
            data (pandas.DataFrame): The DataFrame to be exported.
            output_dir (str): The directory to save the CSV files.
            custom_file_name (str, optional): Custom file name prefix. Defaults to None.
        """
        try:
            # Ensure the output directory exists
            if not self.fs.exists(output_dir):
                self.fs.makedirs(output_dir, exist_ok=True)

            data["count_date"] = pd.to_datetime(data["count_date"])

            if custom_file_name == "hex_3hourly_counts":
                # the line below added to add double quotes around hours
                # because excel autoformats it to date
                data["time_indicator"] = "'" + data["time_indicator"]
            elif custom_file_name == "MRLI_3yr_compressed":
                data["hours"] = "'" + data["hours"]

            for year, group in data.groupby(data["count_date"].dt.year):
                if custom_file_name:
                    file_name = os.path.join(
                        output_dir, f"{custom_file_name}_{year}.csv").replace("\\", "/")
                else:
                    file_name = os.path.join(
                        output_dir, f"hex_3hourly_counts_{year}.csv").replace("\\", "/")
                with self.fs.open(file_name, 'w', encoding='utf-8', newline="") as f:
                    group.to_csv(f, index=False)
            # Revert modifications after writing to csv
            if custom_file_name == "hex_3hourly_counts":
                data["time_indicator"] = data["time_indicator"].str.strip("'")
            elif custom_file_name == "MRLI_3yr_compressed":
                data["hours"] = data["hours"].str.strip("'")
                logging.info(f"Saved {file_name}")
        except Exception as e:
            logging.error(f"An error occurred while writing CSV files: {e}")

    def write_to_csv_by_year_half(self, data, output_dir, custom_file_name=None):
        """
            Writes a DataFrame to CSV files by year, with each year split into two
            6-month CSV files. Handles both local and S3 file systems.

            Args:
                data (pandas.DataFrame): The DataFrame to be exported.
                output_dir (str): The directory to save the CSV files (local or S3).
                custom_file_name (str, optional): Custom file name prefix.
                Defaults to None.
        """
        # Check if the path is S3 or local
        if output_dir.startswith("s3://"):
            fs = fsspec.filesystem("s3")
        else:
            fs = fsspec.filesystem("file")
        # Ensure output_dir exists
        # fs = fsspec.filesystem("file" if output_dir.startswith("/") else "s3")
        # if not fs.exists(output_dir):
        #     fs.makedirs(output_dir)
        #     logging.info(f"Created output directory: {output_dir}")

        data = data.copy()  # Avoid modifying the original DataFrame

        data["count_date"] = pd.to_datetime(data["count_date"])

        # Add prefix modifications based on file name type
        if custom_file_name == "hex_3hourly_counts":
            data["time_indicator"] = "'" + data["time_indicator"]
        elif custom_file_name == "MRLI_3yr_compressed":
            data["hours"] = "'" + data["hours"]

        # Create output directory if it doesn't exist
        # fs = fsspec.filesystem("file" if output_dir.startswith("/") else "s3")

        for year, group in data.groupby(data["count_date"].dt.year):
            # Split data into two halves
            first_half = group[group["count_date"].dt.month <= 6]
            second_half = group[group["count_date"].dt.month > 6]

            if custom_file_name:
                file_name_first_half = os.path.join(
                    output_dir, f"{custom_file_name}_{year}_H1.csv").replace("\\", "/")
                file_name_second_half = os.path.join(
                    output_dir, f"{custom_file_name}_{year}_H2.csv").replace("\\", "/")
            else:
                file_name_first_half = os.path.join(
                    output_dir, f"hex_3hourly_counts_{year}_H1.csv").replace("\\", "/")
                file_name_second_half = os.path.join(
                    output_dir, f"hex_3hourly_counts_{year}_H2.csv").replace("\\", "/")

            # Write to CSV only if there's data
            if not first_half.empty:
                with fs.open(file_name_first_half, "w") as f:
                    first_half.to_csv(f, index=False)
                # first_half.to_csv(file_name_first_half, index=False)
                logging.info(f"Saved {file_name_first_half}")
            else:
                logging.info(f"No data for first half of "
                             f"{year}, skipping {file_name_first_half}")

            if not second_half.empty:
                with fs.open(file_name_second_half, "w") as f:
                    second_half.to_csv(f, index=False)
                # second_half.to_csv(file_name_second_half, index=False)
                logging.info(f"Saved {file_name_second_half}")
            else:
                logging.info(f"No data for second"
                             f" half of {year}, skipping {file_name_second_half}")

        # Revert modifications after writing to csv
        if custom_file_name == "hex_3hourly_counts":
            data["time_indicator"] = data["time_indicator"].str.strip("'")
        elif custom_file_name == "MRLI_3yr_compressed":
            data["hours"] = data["hours"].str.strip("'")
            logging.info(f"Reverted modifications for {custom_file_name}")

    def write_threehourly_hs_to_csv(self, data, data_source):
        """
        Writes a DataFrame to a CSV file based on the first column's name and data type.

        Args:
            data (pandas.DataFrame): The DataFrame to be exported.
            data_source (str): The type of data ("mastercard" or "bt").

        Raises:
            FileNotFoundError: If the specified file or directory is not found.
            PermissionError: If there's a permission issue when writing the file.
            Exception: For any other unexpected errors during the export process.
        """
        try:
            if data_source in self.hs_file_path:
                first_column_name = data.columns[0]
                if first_column_name in [
                    "highstreet_id",
                    "tc_id",
                    "bespoke_area_id",
                    "bid_id",
                    "msoa_id",
                    "lsoa_id",
                ]:
                    directory_name = {
                        "highstreet_id": "highstreet",
                        "tc_id": "towncentre",
                        "bespoke_area_id": "bespoke",
                        "bid_id": "bid",
                        "msoa_id": "msoa",
                        "lsoa_id": "lsoa",
                    }[first_column_name]
                    start_date = data["count_date"].min().strftime("%Y-%m-%d")
                    end_date = data["count_date"].max().strftime("%Y-%m-%d")
                    # the line below added to add double quotes around hours
                    # for hex level databecause excel autoformats it to date
                    if first_column_name != "msoa_id" and first_column_name != "lsoa_id":
                        data["hours"] = "'" + data["hours"]
                        modify_in_place = True
                    else:
                        modify_in_place = False
                    if first_column_name == "msoa_id":
                        filename = (
                            f"{directory_name}_hourly_counts_"
                            f"{start_date}_{end_date}.csv"
                        )
                    if first_column_name == "lsoa_id":
                        filename = (
                            f"{directory_name}_hourly_counts_"
                            f"{start_date}_{end_date}.csv"
                        )
                    else:
                        if data_source == "mastercard_3hourly":
                            filename = (
                                f"{directory_name}_3hourly_txn_"
                                f"{start_date}_{end_date}.csv"
                            )
                        else:
                            filename = (
                                f"{directory_name}_3hourly_counts_"
                                f"{start_date}_{end_date}.csv"
                            )
                    file_path = os.path.join(
                        self.hs_file_path[data_source], directory_name, filename
                    ).replace("\\", "/")
                    if modify_in_place:
                        pd.DataFrame(data).to_csv(file_path, index=False)
                        data["hours"] = data["hours"].str.strip("'")
                    else:
                        pd.DataFrame(data).to_csv(file_path, index=False)
                    logging.info(f"Data successfully written to CSV: {file_path}")
                else:
                    logging.error(f"Invalid column name: {first_column_name}")
            else:
                raise ValueError(
                    "Invalid data_source. Supported values are 'mastercard' and 'bt'."
                )

        except FileNotFoundError as e:
            logging.error(f"File not found: {e}")
        except PermissionError as e:
            logging.error(f"Permission error: {e}")
        except Exception as e:
            logging.error(f"Error while writing data to CSV: {e}")

    def truncate_and_load_to_postgres(
        self, dataframe, table_name, schema="public", if_exists="replace", index=False
    ):
        """
        Truncate and load data from a DataFrame into a PostgreSQL table.

        Args:
            dataframe (pd.DataFrame): The DataFrame containing the data to be loaded.
            table_name (str): The name of the PostgreSQL table.
            schema (str, optional): The schema where the table resides.
                                    Default is 'public'.
            if_exists (str, optional): Action to take if the table already exists (
                'fail', 'replace', or 'append').
                Default is 'replace'.
            index (bool, optional): Whether to include the DataFrame index as a
                                    column in the table. Default is False.
            engine (sqlalchemy.engine.Engine, optional): An existing SQLAlchemy engine.
                                    If None, a new engine will be created.

        Returns:
            None
        """
        try:
            # Truncate the existing table
            with self.engine.connect() as connection:
                connection.execute(text(f"TRUNCATE TABLE {schema}.{table_name}"))

            # Load the data from the DataFrame to the PostgreSQL table
            dataframe.to_sql(
                table_name,
                con=self.engine,
                if_exists=if_exists,
                schema=schema,
                index=index,
            )
            logging.info(f"Data loaded successfully into {schema}.{table_name}")

        except Exception as e:
            logging.error(
                f"An error occurred while loading data into {schema}.{table_name}:"
                f" {str(e)}"
            )

    def upload_data_to_lds(
        self,
        slug,
        resource_title,
        source=None,
        poi_type=None,
        df=None,
        file_path=None,
        file_name=None,
        custom_date_column="count_date",  # Added parameter for custom date column
    ):
        """
        Upload data to the Linked Data Service (LDS) for a given resource.

        Parameters:
        - slug (str): The slug identifier for the dataset.
        - resource_title (str): The title of the resource to be uploaded.
        - source (str, optional): The data source identifier. Default is None.
        - poi_type (str, optional): The point of interest type.
          highstreet/bid/towncentre/bespoke/msoa
          Default is None.
        - df (pd.DataFrame, optional): The DataFrame containing the data to be uploaded.
          Default is None.
        - file_path (str, optional): The path to the file if not using a DataFrame.
          Default is None.
        - file_name (str, optional): The name of the file to be uploaded.
          Default is None.

        Raises:
        - ValueError: If neither DataFrame nor file path is provided, or if the
          'count_date' column is not found in the DataFrame.

        Note:
        - If a DataFrame is provided, it calculates 'temporal_coverage_from' and
          'temporal_coverage_to'.
        - If 'file_path' is not provided, a default path is constructed based on the
          provided parameters.
        - It uses the Linked Data Service (LDS) API to replace the resource.
        - Logs successful or failed data upload attempts with appropriate messages.

        Example:
        ```
        lds_instance.upload_data_to_lds(slug='dataset_slug', resource_title='Resource1',
                                        source='BT', poi_type='bid', df=data_frame)
        ```

        """
        if df is None and file_path is None:
            raise ValueError("Either a DataFrame or a file path must be provided.")

        if df is not None:
            # If DataFrame is provided, calculate temporal_coverage_from
            # and temporal_coverage_to
            if custom_date_column not in df.columns:
                raise ValueError(
                    f"Date column {custom_date_column!r} not found in the DataFrame."
                )
            temporal_coverage_from = str(
                pd.to_datetime(df[custom_date_column]).min().date()
            )
            temporal_coverage_to = str(
                pd.to_datetime(df[custom_date_column]).max().date()
            )

        if file_path is not None and df is None:
            # Read the file into a DataFrame
            with fsspec.open(file_path, mode="rt") as file:
                df = pd.read_csv(file)
            if custom_date_column not in df.columns:
                raise ValueError(
                    f"Date column {custom_date_column!r} not found in the file."
                )
            temporal_coverage_from = str(
                pd.to_datetime(df[custom_date_column]).min().date()
            )
            temporal_coverage_to = str(
                pd.to_datetime(df[custom_date_column]).max().date()
            )

        # Construct default file path if not provided
        if file_path is None:
            if source == 'mastercard_3hourly':
                file_path = (
                    f"{self.base_path}mastercard/mrli_3hourly/processed/{poi_type}/"
                    f"{file_name}_{temporal_coverage_from}"
                    f"_{temporal_coverage_to}.csv"
                )
            elif source == 'bt':
                file_path = (
                    f"{self.base_path}{source}/processed/{poi_type}/"
                    f"{file_name}_{temporal_coverage_from}"
                    f"_{temporal_coverage_to}.csv"
                )
        try:
            # Replace resource using lds.replace_resource
            df_metadata = lds.meta_dataset(slug, self.lds_api_key)[
                ["resource_id", "resource_title", "description_y"]
            ]
            resource_id = df_metadata.loc[
                df_metadata["resource_title"] == resource_title, "resource_id"
            ].values[0]
            resource_description = df_metadata.loc[
                df_metadata["resource_title"] == resource_title, "description_y"
            ].values[0]
            if df is not None:
                lds.replace_resource_s3(
                    file_path=file_path,
                    slug=slug,
                    api_key=self.lds_api_key,
                    res_id=resource_id,
                    temporal_coverage_from=pd.to_datetime(temporal_coverage_from),
                    temporal_coverage_to=pd.to_datetime(temporal_coverage_to),
                    res_title=resource_title,
                    description=resource_description,
                )
                logging.info(
                    f"Data uploaded successfully to LDS for resource {resource_title}"
                    f"from {temporal_coverage_from} to {temporal_coverage_to}"
                )
            else:
                lds.replace_resource_s3(
                    file_path=file_path,
                    slug=slug,
                    api_key=self.lds_api_key,
                    res_title=resource_title,
                    res_id=resource_id,
                    description=resource_description,
                )
                logging.info(
                    f"Data uploaded successfully to LDS for resource {resource_title}"
                )
        except Exception as e:
            logging.error(
                f"Failed to upload data to LDS for resource '"
                f"{resource_title}: {str(e)}"
            )
