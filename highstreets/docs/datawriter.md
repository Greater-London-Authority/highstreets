# Data Writer Documentation

## Introduction

The `DataWriter` class serves as a crucial component in our data engineering toolkit, playing a pivotal role in managing the storage, organization, and dissemination of data across various stages of our data pipeline. Its main objective is to facilitate efficient data storage, both in CSV files and in our PostgreSQL database, while ensuring data integrity and optimizing storage workflows.

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

The `load_data_to_csv` method addresses the need to store data in CSV files. It accepts a Pandas DataFrame and a file path as inputs. The method efficiently writes the DataFrame to the specified file path as a CSV, capturing the data in a portable format that can be easily shared or transferred. This feature is valuable for generating data snapshots and intermediate outputs.

```python
def load_data_to_csv(self, data, file_path):
    try:
        data.to_csv(file_path, index=False)
        logging.info(f"Data successfully loaded to CSV: {file_path}")
    except Exception as e:
        logging.error(f"Error while loading data to CSV: {e}")
```
## Checking Table Existence
The `table_exists` method checks whether a given table exists in the PostgreSQL database. It returns `True` if the table exists, and `False` otherwise. It acts as an interface to check the existence of a table within the PostgreSQL database. This verification is crucial before attempting data appending operations, preventing potential errors or data loss due to mismatches between table names and existing data structures.

```python
def table_exists(self, table_name):
    return self.engine.dialect.has_table(self.engine.connect(), table_name)
```
## Appending Data to PostgreSQL
The heart of the class lies in the `append_data_to_postgres` method. It systematically handles the appending of data to existing PostgreSQL tables. It begins by verifying the existence of the target table. If the table exists, it retrieves the maximum date from the table to establish a starting point for new data. The method then filters incoming data to include only rows with dates later than the maximum date in the table. This intelligent filtering prevents redundant data storage.

If new data exists after filtering, the method appends the filtered data to the PostgreSQL table. This append operation ensures data continuity while maintaining the integrity of existing records. The method is designed to seamlessly integrate with different data categories, making it versatile and adaptable to our evolving data needs.

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
To further enhance data quality, the `append_data_with_id_check` method allows for data appending with an ID check. This is particularly relevant when dealing with unique identifiers, such as IDs for highstreets, town centres, bids, and bespoke areas. Before appending, the method checks if the incoming data contains IDs that already exist in the target table. If any conflict arises, the method logs an error, preventing duplicate or conflicting data from being inserted.

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
For data archival and analysis purposes, the `write_hex_to_csv_by_year` method facilitates the organization of data into CSV files based on the count date's year. This systematic storage allows for efficient data retrieval and analysis, contributing to the maintainability of our data repository.
```python
def write_hex_to_csv_by_year(self, data, output_dir):
    # ...
```
## Writing Three-Hourly HS Data to CSV
The `write_threehourly_hs_to_csv` method handles the generation of CSV files specifically tailored to three-hourly data. It employs a dynamic approach, naming files based on the data category (e.g., highstreets, town centres) and the date range. This organized storage promotes accessibility and simplifies the data browsing experience.
```python
def write_threehourly_hs_to_csv(self, data):
    # ...
```
## Summary
In summary, the `DataWriter` class significantly enhances the efficiency, reliability, and organization of our data management processes. By facilitating seamless data storage, integrating with our PostgreSQL database, and providing features like ID checks and checkpointing, it empowers data engineers to confidently manage data at various stages of our pipeline. The class is adaptable and extensible, designed to accommodate diverse data categories and evolving requirements. Its well-defined methods and modular structure ensure a consistent approach to data storage, while its integration with environment variables enhances security and configurability. Overall, the DataWriter class contributes to a robust and effective data infrastructure, enabling our team to maintain the integrity and availability of our data assets.















