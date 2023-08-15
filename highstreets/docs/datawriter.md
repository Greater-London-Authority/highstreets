# Data Writer Documentation

## Introduction

This comprehensive document provides detailed insights into the functionalities and features of the `DataWriter` class. This class is designed to handle data writing tasks, including loading data to CSV files, appending data to PostgreSQL databases, and generating CSV files based on certain conditions.

## DataWriter Class

The `DataWriter` class serves as a versatile tool for various data writing operations. It facilitates loading data to CSV files, appending data to PostgreSQL databases, and other data-related tasks.

Initialization
Upon initialization, the class sets up the necessary database connection parameters using environment variables. It establishes a connection to the PostgreSQL database through the SQLAlchemy engine.

```python
self.database = os.getenv("PG_DATABASE")
self.username = os.getenv("PG_USER")
self.password = os.getenv("PG_PASSWORD")
self.host = os.getenv("PG_HOST")
self.port = os.getenv("PG_PORT")
self.engine = create_engine(
    f"postgresql+psycopg2://{self.username}:{self.password}@"
    f"{self.host}:{self.port}/{self.database}"
)
self.hs_file_path = "/mnt/q/Projects/2019-20/Covid-19 Busyness/data/BT/"
```
## Loading Data to CSV

The `load_data_to_csv` method allows data to be loaded into CSV files. It takes data and a file path as input, writes the data to the specified file path, and logs the outcome.

```python
def load_data_to_csv(self, data, file_path):
    try:
        data.to_csv(file_path, index=False)
        logging.info(f"Data successfully loaded to CSV: {file_path}")
    except Exception as e:
        logging.error(f"Error while loading data to CSV: {e}")
```
## Checking Table Existence
The `table_exists` method checks whether a given table exists in the PostgreSQL database. It returns `True` if the table exists, and `False` otherwise.

```python
def table_exists(self, table_name):
    return self.engine.dialect.has_table(self.engine.connect(), table_name)
```
## Appending Data to PostgreSQL
The `append_data_to_postgres` method appends data to an existing table in the PostgreSQL database. It performs several key steps, including filtering data based on the max date in the table and executing the appending process.

```python
def append_data_to_postgres(self, data, table_name):
    # ...
```
## Appending Data Without Check
The append_data_without_check method directly appends data to an existing table without any prior checks. It simplifies the process of appending data when no ID or date-based checks are required.

```python
def append_data_without_check(self, data, table_name):
    # ...
```
## Appending Data With ID Check
The append_data_with_id_check method appends data to a table with a check for existing IDs. If the IDs already exist in the table, it logs an error message and does not append the data. Otherwise, it appends the data to the table.

```python
def append_data_with_id_check(self, data, ids, id_col, table_name):
    # ...
```
## Loading HS/DS Lookup Data to PostgreSQL

The 'load_hsds_lookup_to_postgres' method is designed to load highstreet, town centre, and bid lookup data into PostgreSQL. It constructs GeoDataFrames from SQL queries, adds a "layer" column, and then merges these dataframes into a single dataframe. The final dataframe is written to a table in PostgreSQL.
```python
def load_hsds_lookup_to_postgres(self):
    # ...
```
## Writing Hex Data to CSV by Year
The `write_hex_to_csv_by_year` method generates and saves CSV files for hex data grouped by year. It takes a dataframe and an output directory as input, and creates separate CSV files for each year within the specified dataframe.
```python
def write_hex_to_csv_by_year(self, data, output_dir):
    # ...
```
## Writing Three-Hourly HS Data to CSV
The write_threehourly_hs_to_csv method writes three-hourly data for highstreets, town centres, bids, and bespoke areas to CSV files. The file name is dynamically generated based on the data being processed.
```python
def write_threehourly_hs_to_csv(self, data):
    # ...
```
## Summary
The `DataWriter` class offers a comprehensive set of data writing functionalities. It covers loading data to CSV files, appending data to PostgreSQL databases, and generating CSV files based on specific conditions. This documentation serves as a valuable resource for effectively utilizing the data writing capabilities of the `DataWriter` class in a variety of scenarios.
















