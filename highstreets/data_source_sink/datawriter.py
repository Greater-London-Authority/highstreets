import logging
import os
import boto3
import fsspec
import psycopg2
import io
import geopandas as gpd
import pandas as pd
from datetime import date
from dotenv import find_dotenv, load_dotenv
from glapy.database.utils import fast_write
from sqlalchemy import create_engine, text
import tempfile
import shutil
from datapress import DataPressClient
from typing import Dict, Any
import time
import random

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

        # Add datapress client initialization
        self.datapress_client = None
        self._initialize_datapress_client()

    def _initialize_datapress_client(self):
        """Initialize the DataPress client for London Data Store uploads."""
        try:
            datapress_api_key = os.getenv("LDS_API_KEY")
            datapress_url = "https://data.london.gov.uk"

            if datapress_api_key and datapress_url:
                self.datapress_client = DataPressClient(
                    api_key=datapress_api_key,
                    base_url=datapress_url
                )
                logging.info("DataPress client initialized successfully")
            else:
                logging.warning(
                    "DataPress credentials not found in environment variables")
        except Exception as e:
            logging.error(f"Failed to initialize DataPress client: {str(e)}")
            self.datapress_client = None

    def _retry_operation(self, operation, max_retries=3, operation_name="operation"):
        """Generic retry wrapper with exponential backoff."""
        for attempt in range(max_retries):
            try:
                return operation()
            except Exception as e:
                if attempt == max_retries - 1:  # Last attempt
                    logging.error(
                        f"{operation_name} failed after"
                        f" {max_retries} attempts: {str(e)}")
                    raise

                # Exponential backoff with jitter
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logging.warning(f"{operation_name} failed (attempt"
                                f" {attempt + 1}): {str(e)}")
                logging.info(f"Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)

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

    def write_quad_lookup_to_postgres(self, df, layer_type):
        """
        Write Mastercard quad lookup table to PostgreSQL using high-performance methods.

        Args:
            df (pandas.DataFrame): DataFrame containing lookup data
            layer_type (str): Type of layer (e.g., 'borough', 'highstreet', 'bespoke')

        Returns:
            bool: True if successful, False otherwise

        Raises:
            ValueError: If an invalid layer_type is provided
        """
        import time
        from io import StringIO
        from sqlalchemy.exc import (ProgrammingError, OperationalError)

        start_time = time.time()

        # Validate layer_type
        valid_layer_types = [
            'borough', 'bespoke', 'highstreet', 'bid',
            'msoa', 'towncentre', 'caz', 'inner_outer',
            'BIDs', 'CAZ', 'Highstreets', 'TownCentres',
            'Boroughs', 'MSOAs', 'london', 'Inner_Outer'
        ]

        if layer_type not in valid_layer_types:
            error_msg = (
                f"Invalid layer_type: {layer_type}. "
                f"Valid types are: {valid_layer_types}"
            )
            logging.error(error_msg)
            raise ValueError(error_msg)

        # Validate DataFrame
        required_columns = ['quad_id']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            error_msg = f"DataFrame missing required columns: {missing_columns}"
            logging.error(error_msg)
            raise ValueError(error_msg)

        # Handle empty DataFrame
        if df.empty:
            logging.warning(
                f"Empty DataFrame provided for {layer_type} lookup. No data written."
            )
            return False

        # Format the table name according to convention
        if layer_type == 'inner_outer':
            table_name = 'econ_busyness_mcard_inner_outer_quad_lookup'
        else:
            # layer_type_lower = layer_type.lower()
            table_name = f'econ_busyness_mcard_{layer_type}_quad_lookup'

        schema = 'gisapdata'
        full_table_name = f"{schema}.{table_name}"

        logging.info(f"Writing {len(df):,} rows to {full_table_name}...")

        conn = None
        success = False
        max_retries = 3
        retry_count = 0

        while retry_count <= max_retries:
            try:
                # Create a connection from the engine
                conn = self.engine.raw_connection()

                # Begin a transaction
                conn.autocommit = False
                cursor = conn.cursor()

                # Drop the table if it exists (for clean replacement)
                cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")

                # Create column definition based on DataFrame dtypes
                columns = []
                for col_name, dtype in df.dtypes.items():
                    if "int" in str(dtype):
                        columns.append(f"{col_name} INTEGER")
                    elif "float" in str(dtype):
                        columns.append(f"{col_name} DOUBLE PRECISION")
                    elif "datetime" in str(dtype):
                        columns.append(f"{col_name} TIMESTAMP")
                    else:
                        columns.append(f"{col_name} TEXT")

                # Create the table
                create_stmt = (
                    f"CREATE TABLE {full_table_name} ({', '.join(columns)})"
                )
                cursor.execute(create_stmt)

                # Prepare data as CSV in memory
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False, header=True)
                csv_buffer.seek(0)

                # Skip header row
                next(csv_buffer)

                # Execute COPY command
                cursor.copy_expert(
                    f"COPY {full_table_name} FROM STDIN WITH CSV",
                    csv_buffer
                )

                # Commit the transaction
                conn.commit()
                success = True

                elapsed_time = time.time() - start_time
                logging.info(
                    f"Successfully wrote {len(df):,} rows to {full_table_name} "
                    f"in {elapsed_time:.2f} seconds"
                )

                break  # Exit the retry loop on success

            except (ProgrammingError, OperationalError) as e:
                if conn and not conn.closed:
                    conn.rollback()

                retry_count += 1
                if retry_count <= max_retries:
                    logging.warning(
                        f"Database error, retrying"
                        f" ({retry_count}/{max_retries}): {str(e)}"
                    )
                    time.sleep(1)  # Add delay before retry
                else:
                    logging.error(
                        f"Failed after {max_retries} attempts: {str(e)}"
                    )
                    break

            except Exception as e:
                if conn and not conn.closed:
                    conn.rollback()
                error_msg = f"Error writing to {full_table_name}: {str(e)}"
                logging.error(error_msg)
                success = False
                break

            finally:
                if conn and not conn.closed:
                    conn.close()

        return success

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

    def export_table_by_year_to_s3(self, table_name, date_column, s3_base_path,
                                   file_prefix=None, latest=False,
                                   apostrophe_columns=None):
        """
        Exports data from a PostgreSQL table into yearly partitions as CSV files on S3.

        Parameters:
        table_name (str): The name of the PostgreSQL table.
        date_column (str): The column in the table containing the date/timestamp for
        partitioning.
        s3_base_path (str): The base S3 path where the CSV files will be written
                            (e.g., "s3://your-bucket/path/to/chunks").
        file_prefix (str): The prefix to use for CSV filenames (default is
        "hex_3hourly_counts").
        latest (bool): If True, only exports the latest year's data. Default is False.
        apostrophe_columns (list): List of column names to prefix with apostrophe.

        The function:
        1. Determines the full date range from the table.
        2. Partitions the data by year (or just latest year if latest=True).
        3. Uses PostgreSQL's COPY command to stream each partition directly to S3.
        """
        try:
            # Set default file prefix if none provided
            if file_prefix is None:
                file_prefix = "hex_3hourly_counts"

            # Connect to PostgreSQL using credentials from environment variables
            conn = psycopg2.connect(
                dbname=self.database,
                host=self.host,
                user=self.username,
                password=self.password,
                port=self.port
            )
            cur = conn.cursor()

            # Determine the full date range for the provided date column
            query = f"SELECT MIN({date_column}), MAX({date_column}) FROM {table_name};"
            cur.execute(query)
            min_date, max_date = cur.fetchone()

            if not min_date or not max_date:
                raise ValueError("Table is empty or the date column is not populated.")

            start_year = max_date.year if latest else min_date.year
            end_year = max_date.year

            # Build the SELECT statement with optional apostrophe formatting
            if apostrophe_columns:
                # Get all columns in their original order
                all_columns_query = f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """

                cur.execute(all_columns_query)
                all_columns = [row[0] for row in cur.fetchall()]

                # Build SELECT list maintaining original order
                select_parts = []
                for col in all_columns:
                    if col in apostrophe_columns:
                        select_parts.append(
                            f"'''' || COALESCE({col}::text, '') AS {col}")
                    else:
                        select_parts.append(col)
                select_statement = ', '.join(select_parts)
            else:
                select_statement = "*"

            # Initialize S3 filesystem via fsspec
            fs = fsspec.filesystem("s3")

            # Loop over each year in the range
            for year in range(start_year, end_year + 1):
                # Construct the file name
                file_name = f"{file_prefix}_{year}.csv"
                s3_file_path = f"{s3_base_path.rstrip('/')}/{file_name}"

                # Build the COPY command query with optimized date filtering
                copy_sql = f"""
                    COPY (
                        SELECT {select_statement}
                        FROM {table_name}
                        WHERE {date_column} >= '{year}-01-01'::date
                        AND {date_column} < '{year+1}-01-01'::date
                        ORDER BY {date_column}
                    ) TO STDOUT WITH CSV HEADER;
                """

                logging.info(f"Exporting data for year {year} to {s3_file_path}...")

                try:
                    # Open the S3 file for writing and stream the data
                    with fs.open(s3_file_path, 'w') as s3_file:
                        cur.copy_expert(copy_sql, s3_file)
                    logging.info(f"Successfully exported: {file_name}")
                except Exception as e:
                    logging.error(f"Error exporting {file_name}: {str(e)}")
                    continue

        except Exception as e:
            logging.error(f"Error in export_table_by_year_to_s3: {str(e)}")
        finally:
            # Clean up the connection
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()
            logging.info("Database connection closed")

    def get_year_range(self, table_name, date_column):
        """
        Returns a list of years spanning MIN to MAX of a date column.

        Parameters:
            table_name (str): PostgreSQL table name.
            date_column (str): Column containing date/timestamp values.

        Returns:
            list[int]: List of years from min to max (inclusive).
        """
        try:
            conn = psycopg2.connect(
                dbname=self.database,
                host=self.host,
                user=self.username,
                password=self.password,
                port=self.port
            )
            cur = conn.cursor()
            cur.execute(
                f"SELECT MIN({date_column}), MAX({date_column}) FROM {table_name};")
            min_date, max_date = cur.fetchone()
            if not min_date or not max_date:
                logging.warning(f"Table {table_name} is empty or {date_column} "
                                "is not populated.")
                return []
            return list(range(min_date.year, max_date.year + 1))
        except Exception as e:
            logging.error(f"Error in get_year_range: {str(e)}")
            return []
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()

    def get_distinct_values(self, table_name, column_name):
        """
        Returns sorted distinct values of a column from a table.

        Parameters:
            table_name (str): PostgreSQL table name.
            column_name (str): Column to retrieve distinct values from.

        Returns:
            list[str]: Sorted list of distinct values.
        """
        try:
            conn = psycopg2.connect(
                dbname=self.database,
                host=self.host,
                user=self.username,
                password=self.password,
                port=self.port
            )
            cur = conn.cursor()
            cur.execute(
                f"SELECT DISTINCT {column_name} FROM {table_name} "
                f"ORDER BY {column_name};")
            return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logging.error(f"Error in get_distinct_values: {str(e)}")
            return []
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()

    def export_table_by_partition_to_s3(self, table_name, partition_column,
                                        s3_base_path, file_prefix,
                                        source_query=None):
        """
        Exports data partitioned by distinct values of a categorical column
        as separate CSV files on S3.

        Parameters:
            table_name (str): Base PostgreSQL table (used to discover partition
                values).
            partition_column (str): Column to partition by.
            s3_base_path (str): S3 base path for output files.
            file_prefix (str): Prefix for CSV filenames. Output files are named
                {file_prefix}_{partition_value}.csv with spaces replaced by
                underscores.
            source_query (str, optional): If provided, wraps this query in a CTE
                and exports the enriched result. The query should SELECT from the
                base table aliased as 'd' and must NOT include a trailing
                semicolon. If None, exports directly from the table.
        """
        try:
            conn = psycopg2.connect(
                dbname=self.database,
                host=self.host,
                user=self.username,
                password=self.password,
                port=self.port
            )
            cur = conn.cursor()

            cur.execute(
                f"SELECT DISTINCT {partition_column} FROM {table_name} "
                f"ORDER BY {partition_column};")
            partition_values = [row[0] for row in cur.fetchall()]

            if not partition_values:
                logging.warning(f"No distinct values found for {partition_column} "
                                f"in {table_name}.")
                return

            logging.info(f"Found {len(partition_values)} partitions: "
                         f"{partition_values}")

            fs = fsspec.filesystem("s3")

            for val in partition_values:
                safe_val = val.replace(' ', '_')
                file_name = f"{file_prefix}_{safe_val}.csv"
                s3_file_path = f"{s3_base_path.rstrip('/')}/{file_name}"

                if source_query:
                    copy_sql = f"""
                        COPY (
                            WITH enriched AS ({source_query})
                            SELECT * FROM enriched
                            WHERE {partition_column} = '{val}'
                            ORDER BY count_date
                        ) TO STDOUT WITH CSV HEADER;
                    """
                else:
                    copy_sql = f"""
                        COPY (
                            SELECT * FROM {table_name}
                            WHERE {partition_column} = '{val}'
                            ORDER BY 1
                        ) TO STDOUT WITH CSV HEADER;
                    """

                logging.info(f"Exporting partition '{val}' to {s3_file_path}...")

                try:
                    with fs.open(s3_file_path, 'w') as s3_file:
                        cur.copy_expert(copy_sql, s3_file)
                    logging.info(f"Successfully exported: {file_name}")
                except Exception as e:
                    logging.error(f"Error exporting {file_name}: {str(e)}")
                    continue

        except Exception as e:
            logging.error(f"Error in export_table_by_partition_to_s3: {str(e)}")
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()
            logging.info("Database connection closed")

    def export_table_by_year_half_to_s3(self, table_name, date_column, s3_base_path,
                                        file_prefix=None):
        """
        Exports data from a PostgreSQL table into half-year partitions
        as CSV files on S3.

        Parameters:
        table_name (str): The name of the PostgreSQL table.
        date_column (str): The column in the table containing the date/timestamp
        for partitioning.
        s3_base_path (str): The base S3 path where the CSV files will be written
        (e.g., "s3://your-bucket/path/to/chunks").
        file_prefix (str): The prefix to use for CSV filenames (default is None).

        The function:
        1. Determines the full date range from the table.
        2. Partitions the data by year and by half-year:
            - H1: January 1 to June 30
            - H2: July 1 to December 31
        3. Uses PostgreSQL's COPY command to stream each partition directly to S3.
        """
        # Connect to PostgreSQL using credentials from environment variables.
        conn = psycopg2.connect(
            dbname=self.database,
            host=self.host,
            user=self.username,
            password=self.password,
            port=self.port
        )
        cur = conn.cursor()

        # Determine the full date range for the provided date column.
        query = f"SELECT MIN({date_column}), MAX({date_column}) FROM {table_name};"
        cur.execute(query)
        min_date, max_date = cur.fetchone()

        if not min_date or not max_date:
            raise ValueError("Table is empty or the date column is not populated.")

        start_year = min_date.year
        end_year = max_date.year

        # Initialize S3 filesystem via fsspec.
        fs = fsspec.filesystem("s3")

        # Loop over each year in the range.
        for year in range(start_year, end_year + 1):
            # Define the two half-year date ranges.
            h1_start = date(year, 1, 1)
            h1_end = date(year, 6, 30)
            h2_start = date(year, 7, 1)
            h2_end = date(year, 12, 31)

            partitions = [
                {"label": "H1", "start": h1_start, "end": h1_end},
                {"label": "H2", "start": h2_start, "end": h2_end},
            ]

            for part in partitions:
                # Construct the file name.
                file_name = f"{file_prefix}_{year}_{part['label']}.csv"
                s3_file_path = f"{s3_base_path.rstrip('/')}/{file_name}"

                # Build the COPY command query.
                copy_sql = f"""
                    COPY (
                        SELECT *
                        FROM {table_name}
                        WHERE {date_column} >= '{part['start']}'
                        AND {date_column} <= '{part['end']}'
                    ) TO STDOUT WITH CSV HEADER;
                """

                logging.info(f"Exporting data for {year} {part['label']}"
                             f" to {s3_file_path}...")

                # Open the S3 file for writing and stream the data.
                with fs.open(s3_file_path, 'w') as s3_file:
                    cur.copy_expert(copy_sql, s3_file)

                logging.info(f"Exported: {file_name}")

        # Clean up the connection.
        cur.close()
        conn.close()

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

    def _standardize_columns_for_postgres(self, df, table_name):
        """
        Standardize DataFrame columns to match PostgreSQL table structure.

        Args:
            df (pd.DataFrame): DataFrame to standardize
            table_name (str): Name of the target PostgreSQL table

        Returns:
            pd.DataFrame: DataFrame with standardized columns
        """
        try:
            # Get existing table columns if table exists
            with self.engine.connect() as connection:
                result = connection.execute(text(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_name = '{table_name}' AND "
                    f"table_schema = 'gisapdata' "
                    f"ORDER BY ordinal_position"
                ))
                existing_columns = [row[0] for row in result.fetchall()]

            if not existing_columns:
                # Table doesn't exist, return original DataFrame
                logging.info(
                    f"Table {table_name} doesn't exist, "
                    "will be created with current columns"
                )
                return df

            df_standardized = df.copy()

            # Remove extra columns that don't exist in PostgreSQL table
            extra_columns = [
                col for col in df_standardized.columns
                if col not in existing_columns
            ]
            if extra_columns:
                logging.info(
                    f"Removing extra columns for {table_name}: {extra_columns}"
                )
                df_standardized = df_standardized.drop(columns=extra_columns)

            # Add missing columns with None/NaN values
            missing_columns = [
                col for col in existing_columns
                if col not in df_standardized.columns
            ]
            if missing_columns:
                logging.info(
                    f"Adding missing columns for {table_name}: {missing_columns}"
                )
                for col in missing_columns:
                    df_standardized[col] = None

            # Reorder columns to match PostgreSQL table order
            df_standardized = df_standardized.reindex(columns=existing_columns)

            logging.info(
                f"Standardized {table_name}: {len(df)} rows, "
                f"{len(df_standardized.columns)} columns"
            )
            return df_standardized

        except Exception as e:
            logging.warning(
                f"Could not standardize columns for {table_name}: {str(e)}"
            )
            logging.warning("Proceeding with original DataFrame columns")
            return df

    def truncate_and_load_to_postgres(
        self, dataframe, table_name, schema="gisapdata", index=False,
        standardize_columns=False
    ):
        """
        Truncate and load data from a DataFrame into a PostgreSQL table.
        This preserves the table structure while replacing all data.

        Args:
            dataframe (pd.DataFrame): The DataFrame containing the data to be loaded.
            table_name (str): The name of the PostgreSQL table.
            schema (str, optional): The schema where the table resides.
                                   Default is 'gisapdata'.
            index (bool, optional): Whether to include the DataFrame index as a
                                   column in the table. Default is False.
            standardize_columns (bool, optional): Whether to standardize columns
                                                 to match existing table structure.
                                                 Default is False.

        Returns:
            None
        """
        try:
            # Standardize columns if requested and table exists
            if standardize_columns:
                dataframe = self._standardize_columns_for_postgres(
                    dataframe, table_name
                )

            # First check if the table exists
            with self.engine.connect() as connection:
                result = connection.execute(text(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables "
                    f"WHERE table_schema = '{schema}' AND "
                    f"table_name = '{table_name}')"
                ))
                table_exists = result.scalar()

            if table_exists:
                # If table exists, truncate it first
                with self.engine.connect() as connection:
                    connection.execute(text(f"TRUNCATE TABLE {schema}.{table_name}"))
                    connection.commit()

                # Then append data to the existing (now empty) table structure
                dataframe.to_sql(
                    table_name,
                    con=self.engine,
                    if_exists="append",  # Changed to append since table
                                         # structure is preserved
                    schema=schema,
                    index=index,
                    method='multi',  # For better performance
                    chunksize=5000
                )
            else:
                # If table doesn't exist, create it
                dataframe.to_sql(
                    table_name,
                    con=self.engine,
                    if_exists="replace",  # Create new table
                    schema=schema,
                    index=index
                )

            logging.info(f"Data loaded successfully into {schema}.{table_name}")

        except Exception as e:
            logging.error(
                f"An error occurred while loading data into {schema}.{table_name}:"
                f" {str(e)}"
            )
            raise  # Re-raise the exception for better error handling

    def _upload_file_with_s3_support_streaming(
        self,
        dataset_id: str,
        file_path: str,
        chunk_size: int = 4 * 1024 * 1024,  # 4MB chunks for large files
        show_progress: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Enhanced streaming version that handles large files efficiently.

        Args:
            dataset_id: Dataset ID for the upload
            file_path: File path (local or S3)
            chunk_size: Size of chunks for streaming (default 4MB)
            show_progress: Whether to show download progress
            **kwargs: Additional arguments to pass to client.upload_file()

        Returns:
            Result from client.upload_file()
        """

        if not self.datapress_client:
            raise ValueError("DataPress client not initialized. Check your credentials.")

        if file_path.startswith('s3://'):
            # Create a temporary file with appropriate suffix
            file_extension = os.path.splitext(file_path)[1] or '.csv'

            with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:  # noqa: E501
                temp_path = temp_file.name

            try:
                # Get file size for progress tracking (optional)
                if show_progress:
                    fs = fsspec.filesystem('s3')
                    try:
                        file_size = fs.size(file_path)
                        logging.info(
                            f"Downloading {file_path}"
                            f" ({file_size / 1024 / 1024:.1f} MB)...")
                    except Exception:
                        file_size = None
                        logging.info(f"Downloading {file_path}...")

                # Stream from S3 to local file with custom chunk size
                with fsspec.open(file_path, 'rb') as s3_file:
                    with open(temp_path, 'wb') as local_file:
                        if show_progress and file_size:
                            # Progress tracking version
                            downloaded = 0
                            while True:
                                chunk = s3_file.read(chunk_size)
                                if not chunk:
                                    break
                                local_file.write(chunk)
                                downloaded += len(chunk)
                                progress = (downloaded / file_size) * 100
                                # Log every 10MB
                                if downloaded % (10 * 1024 * 1024) == 0:
                                    logging.info(f"Download progress: {progress:.1f}%")
                        else:
                            # Simple streaming without progress
                            shutil.copyfileobj(s3_file, local_file, length=chunk_size)

                if show_progress:
                    logging.info("Download complete. Uploading to London Data Store...")

                # Upload using datapress client
                result = self.datapress_client.upload_file(
                    dataset_id=dataset_id,
                    file_path=temp_path,
                    **kwargs
                )

                if show_progress:
                    logging.info(f"Upload complete! Resource ID:"
                                 f" {result.get('resource_id', 'N/A')}")

                return result

            except Exception as e:
                logging.error(f"Error processing S3 file {file_path}: {str(e)}")
                raise

            finally:
                # Clean up temporary file
                if os.path.exists(temp_path):
                    try:
                        os.unlink(temp_path)
                    except Exception as e:
                        logging.warning(
                            f"Could not delete temp file {temp_path}: {str(e)}")

        else:
            # For local files, use the client directly
            return self.datapress_client.upload_file(
                dataset_id=dataset_id,
                file_path=file_path,
                **kwargs
            )

    def upload_data_to_lds(  # noqa: C901
        self,
        slug: str,
        resource_title: str,
        file_path: str = None,
        df: pd.DataFrame = None,
        description: str = None,
        custom_date_column: str = "count_date",
        show_progress: bool = True,
        chunk_size: int = 4 * 1024 * 1024
    ):
        """
        Upload data to London Data Store using the DataPress client.
        """

        if not self.datapress_client:
            raise ValueError(
                "DataPress client not initialized. Check your environment variables.")

        if df is None and file_path is None:
            raise ValueError("Either a DataFrame or a file path must be provided.")

        # Get dataset information and find the resource_id with retry
        resource_id = None
        try:
            dataset = self._retry_operation(
                lambda: self.datapress_client.get_dataset(slug),
                operation_name="Get dataset"
            )
            resources = dataset.get('resources', {})

            # Find resource_id by matching resource_title
            for res_id, res_info in resources.items():
                if res_info.get('title') == resource_title:
                    resource_id = res_id
                    break

            if resource_id:
                logging.info(f"Found existing resource ID: {resource_id}"
                             f" for title: {resource_title}")
            else:
                logging.info(f"No existing resource found with title '{resource_title}'"
                             f". Will create new resource.")

        except Exception as e:
            logging.error(f"Failed to get dataset information: {str(e)}")
            raise

        # Always create a properly named temporary file
        temp_df_file = None
        original_file_path = file_path  # Store original for cleanup logic # noqa: F841

        # Sanitize resource_title for use as filename
        safe_filename = "".join(c for c in resource_title if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()  # noqa: E501
        safe_filename = safe_filename.replace(' ', '_')

        # Ensure it has .csv extension if not already present
        if not safe_filename.lower().endswith('.csv'):
            safe_filename += '.csv'

        # Create temporary file with meaningful name
        import tempfile
        import os
        import shutil
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, safe_filename)

        # Remove existing file if it exists, instead of adding timestamp
        if os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
                logging.info(f"Removed existing temporary file: {temp_file_path}")
            except Exception as e:
                logging.warning(f"Could not remove existing temp file: {e}")
                # Only fall back to timestamp if we absolutely
                # can't remove the existing file
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                name_part = safe_filename.rsplit('.', 1)[0]
                ext_part = safe_filename.rsplit('.', 1)[1] if '.' in safe_filename else 'csv'  # noqa: E501
                temp_file_path = os.path.join(
                    temp_dir, f"{name_part}_{timestamp}.{ext_part}")

        try:
            if df is not None:
                # Save DataFrame to properly named temporary file
                df.to_csv(temp_file_path, index=False)
                logging.info(f"DataFrame saved to temporary file: {temp_file_path}")
            elif file_path is not None:
                # Copy existing file to properly named temporary file
                if file_path.startswith('s3://'):
                    # For S3 files, download to properly named temp file
                    with fsspec.open(file_path, 'rb') as s3_file:
                        with open(temp_file_path, 'wb') as local_file:
                            shutil.copyfileobj(s3_file, local_file)
                    logging.info(f"S3 file copied to temporary file: {temp_file_path}")
                else:
                    # For local files, copy to properly named temp file
                    import shutil
                    shutil.copy2(file_path, temp_file_path)
                    logging.info(f"Local file copied to"
                                 f" temporary file: {temp_file_path}")

            # Update file_path to point to our properly named temporary file
            file_path = temp_file_path
            temp_df_file = type('TempFile', (), {'name': temp_file_path})()

            # Calculate timeframe from file or DataFrame
            timeframe = None
            try:
                if df is not None:
                    # Use the DataFrame directly
                    if custom_date_column not in df.columns:
                        raise ValueError(f"Date column {custom_date_column!r}"
                                         f" not found in the DataFrame.")

                    min_date = pd.to_datetime(df[custom_date_column]).min()
                    max_date = pd.to_datetime(df[custom_date_column]).max()
                else:
                    # Read file to get date range
                    with fsspec.open(file_path, mode="rb") as file:
                        temp_df = pd.read_csv(file, encoding="utf-8",
                                              encoding_errors="replace")

                    if custom_date_column not in temp_df.columns:
                        raise ValueError(f"Date column {custom_date_column!r}"
                                         f" not found in the file.")

                    min_date = pd.to_datetime(temp_df[custom_date_column]).min()
                    max_date = pd.to_datetime(temp_df[custom_date_column]).max()

                # Format dates for timeframe
                timeframe = {
                    "from": min_date.strftime("%Y-%m"),
                    "to": max_date.strftime("%Y-%m")
                }

            except Exception as e:
                logging.warning(f"Could not calculate timeframe: {str(e)}")
                # Continue without timeframe if calculation fails

            try:
                # Upload using the streaming method with retry
                upload_kwargs = {
                    "dataset_id": slug,
                    "file_path": file_path,
                    "title": resource_title,
                    "description": description,
                    "timeframe": timeframe,
                    "show_progress": show_progress,
                    "chunk_size": chunk_size
                }

                # Only add resource_id if we found an existing resource
                if resource_id:
                    upload_kwargs["resource_id"] = resource_id

                result = self._retry_operation(
                    lambda: self._upload_file_with_s3_support_streaming(**upload_kwargs),
                    operation_name="File upload"
                )

                if resource_id:
                    logging.info(
                        f"Data uploaded successfully to LDS using DataPress"
                        f" for existing resource"
                        f" '{resource_title}' (Resource ID:"
                        f" {result.get('resource_id', 'N/A')})"
                    )
                else:
                    logging.info(
                        f"Data uploaded successfully to LDS using"
                        f" DataPress as new resource"
                        f" '{resource_title}' (Resource ID:"
                        f" {result.get('resource_id', 'N/A')})"
                    )

                return result

            except Exception as e:
                logging.error(f"Failed to upload data to LDS using"
                              f" DataPress for resource"
                              f" '{resource_title}': {str(e)}")
                raise

        finally:
            # Clean up temporary file if we created one
            if temp_df_file and os.path.exists(temp_df_file.name):
                try:
                    os.unlink(temp_df_file.name)
                    logging.info("Cleaned up temporary file")
                except Exception as e:
                    logging.warning(
                        f"Could not delete temporary file: {str(e)}")

    # Add this new method to the DataWriter class
    def get_latest_s3_file(self, s3_base_path, file_prefix):
        """
        Find the latest file in S3 directory with the given prefix.

        Parameters:
        s3_base_path (str): The S3 base path to search in
        file_prefix (str): The file prefix to match (e.g., 'msoa_hourly_counts')

        Returns:
        str: The full S3 path to the latest file
        """
        try:
            # Remove s3:// prefix and extract bucket and prefix
            s3_path = s3_base_path.replace('s3://', '').strip('/')
            if '/' in s3_path:
                bucket = s3_path.split('/')[0]
                prefix = '/'.join(s3_path.split('/')[1:]) + '/'
            else:
                bucket = s3_path
                prefix = ''

            # List objects with the file prefix
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=f"{prefix}{file_prefix}"
            )

            if 'Contents' not in response:
                raise FileNotFoundError(
                    f"No files found with prefix {file_prefix} in {s3_base_path}")

            # Sort by LastModified and get the latest
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            return f"s3://{bucket}/{latest_file['Key']}"

        except Exception as e:
            logging.error(f"Error finding latest S3 file: {str(e)}")
            raise

    def export_table_to_s3(
        self,
        table_name: str,
        s3_base_path: str,
        file_prefix: str,
        add_date_range_to_filename: bool = False,
        date_column: str = None,
        apostrophe_columns: list = None  # Add this parameter back
    ):
        """
        Exports entire data from a PostgreSQL table as a single CSV file to S3.

        Parameters:
        table_name (str): The name of the PostgreSQL table.
        s3_base_path (str): The base S3 path where the CSV file will be written
                        (e.g., "s3://your-bucket/path/to/data").
        file_prefix (str): The prefix to use for CSV filename.
        add_date_range_to_filename (bool): If True, appends _startdate_enddate to
        filename.
        date_column (str): The name of the date column to use for min/max date.
        apostrophe_columns (list): List of column names to prefix with apostrophe.

        Example:
        export_table_to_s3("my_table", "s3://bucket/folder", "my_data", True,
                       "count_date", ["hours"])
        -> Creates CSV with 'hours column prefixed with apostrophe
        """
        try:
            # Optionally fetch min/max date for filename
            date_range_str = ""
            if add_date_range_to_filename and date_column:
                with self.engine.connect() as conn:
                    result = conn.execute(
                        text(f"SELECT MIN({date_column}) AS min_date, MAX({date_column})"
                             f" AS max_date FROM {table_name}")
                    ).fetchone()
                    min_date = result.min_date
                    max_date = result.max_date
                    if min_date and max_date:
                        # Format as YYYY-MM-DD to remove time part
                        min_date_str = min_date.strftime("%Y-%m-%d")
                        max_date_str = max_date.strftime("%Y-%m-%d")
                        date_range_str = f"_{min_date_str}_{max_date_str}"

            # Construct the file name
            file_name = f"{file_prefix}{date_range_str}.csv"
            s3_file_path = f"{s3_base_path.rstrip('/')}/{file_name}"

            # Connect to PostgreSQL
            conn = psycopg2.connect(
                dbname=self.database,
                host=self.host,
                user=self.username,
                password=self.password,
                port=self.port
            )
            cur = conn.cursor()

            logging.info(f"Exporting data to {s3_file_path}...")

            # Build the COPY command query with optional apostrophe formatting
            if apostrophe_columns:
                # Get all columns in their original order
                all_columns_query = f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """

                with self.engine.connect() as conn:
                    result = conn.execute(text(all_columns_query)).fetchall()
                    all_columns = [row[0] for row in result]

                # Build SELECT list maintaining original order
                select_parts = []
                for col in all_columns:
                    if col in apostrophe_columns:
                        select_parts.append(
                            f"'''' || COALESCE({col}::text, '') AS {col}")
                    else:
                        select_parts.append(col)

                select_statement = ', '.join(select_parts)
            else:
                select_statement = "*"

            copy_sql = f"""
                COPY (
                    SELECT {select_statement}
                    FROM {table_name}
                    ORDER BY 1
                ) TO STDOUT WITH CSV HEADER;
            """

            try:
                # Initialize S3 filesystem via fsspec
                fs = fsspec.filesystem("s3")

                # Open the S3 file for writing and stream the data
                with fs.open(s3_file_path, 'w') as s3_file:
                    cur.copy_expert(copy_sql, s3_file)

                logging.info(f"Successfully exported data to: {s3_file_path}")

            except Exception as e:
                logging.error(f"Error exporting to {s3_file_path}: {str(e)}")
                raise

        except Exception as e:
            logging.error(f"Error in export_table_to_s3: {str(e)}")
            raise

        finally:
            # Clean up the database connection
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()
            logging.info("Database connection closed")
            return s3_file_path

    def append_raw_json_to_monthly_s3(self, raw_data, start_date, end_date,
                                      s3_base_path):
        """
        Appends raw JSON data to monthly files in S3. If the monthly file doesn't exist,
        creates it. If it exists, downloads it, appends the new data, and uploads it
        back.

        Parameters:
        raw_data: The raw JSON data to append (list of dictionaries or JSON string)
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        s3_base_path (str): Base S3 path where monthly JSON files are stored

        Returns:
        list: List of monthly file paths that were updated
        """
        import json
        from datetime import datetime
        import pandas as pd

        try:
            # Ensure raw_data is a list of dictionaries
            if isinstance(raw_data, str):
                raw_data = json.loads(raw_data)
            elif hasattr(raw_data, 'to_dict'):  # pandas DataFrame
                raw_data = raw_data.to_dict('records')

            # Parse date range
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')  # noqa: F841

            # Group data by month
            monthly_data = {}
            for record in raw_data:
                # Extract date from record (assuming 'date' field exists)
                record_date = None
                if 'date' in record:
                    record_date = pd.to_datetime(record['date']).to_pydatetime()
                elif 'count_date' in record:
                    record_date = pd.to_datetime(record['count_date']).to_pydatetime()
                else:
                    # If no date field, use start_date as fallback
                    record_date = start_dt

                # Create monthly key (YYYY-MM)
                month_key = record_date.strftime('%Y-%m')
                if month_key not in monthly_data:
                    monthly_data[month_key] = []
                monthly_data[month_key].append(record)

            # Initialize S3 filesystem
            fs = fsspec.filesystem("s3")
            updated_files = []

            # Process each month
            for month_key, month_records in monthly_data.items():
                # Construct monthly file path
                monthly_filename = f"{month_key}.json"
                s3_monthly_path = f"{s3_base_path.rstrip('/')}/{monthly_filename}"

                existing_data = []

                # Check if monthly file already exists
                try:
                    with fs.open(s3_monthly_path, 'r') as f:
                        existing_data = json.load(f)
                    logging.info(f"Found existing monthly file: {s3_monthly_path}")
                except FileNotFoundError:
                    logging.info(f"Creating new monthly file: {s3_monthly_path}")
                except Exception as e:
                    logging.warning(
                        f"Error reading existing file {s3_monthly_path}: {e}")

                # Combine existing data with new data
                combined_data = existing_data + month_records

                # Remove duplicates if needed (based on a unique field combination)
                # This prevents duplicate data when reprocessing the same date range
                seen = set()
                unique_data = []
                for record in combined_data:
                    # Create a unique key (adjust fields as needed for your data)
                    unique_key = (
                        record.get('poi_id', ''),
                        record.get('date', ''),
                        record.get('time_indicator', '')
                    )
                    if unique_key not in seen:
                        seen.add(unique_key)
                        unique_data.append(record)

                # Sort data by date for better organization
                try:
                    unique_data.sort(key=lambda x: x.get('date', ''))
                except:  # noqa: E722
                    pass  # Skip sorting if date format issues

                # Upload updated monthly file
                with fs.open(s3_monthly_path, 'w') as f:
                    json.dump(unique_data, f, indent=2, default=str)

                updated_files.append(s3_monthly_path)
                logging.info(
                    f"Updated monthly file {s3_monthly_path} with {len(month_records)} "
                    f"new records (total: {len(unique_data)} records)"
                )

            return updated_files

        except Exception as e:
            logging.error(f"Error in append_raw_json_to_monthly_s3: {str(e)}")
            raise
