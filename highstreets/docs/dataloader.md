# Data Loader and Geo Data Lookup Documentation

## Introduction

This detailed document elaborates on the features and functionalities of the `DataLoader` class and its associated get_hex_lookup method for performing geo data lookups. This class is designed to handle the retrieval of data from APIs and PostgreSQL databases, and the `get_hex_lookup` method facilitates associating hex grid data with various types of geographical boundaries.

## DataLoader Class

The `DataLoader` class serves as a central component for fetching and managing data from multiple sources, ensuring data integrity, and establishing database connections.

Initialization
Upon initializing the `DataLoader` class, several key attributes are set up, including the API endpoint, API client, logger, and database connection parameters. These attributes are essential for seamless data retrieval and storage.

```python
self.api_endpoint = "https://api.business.bt.com/v1/footfall/reports/hex-grid/tfl?agg=time_indicator"
self.api_client = APIClient()
self.logger = logging.getLogger(__name__)
self.database = os.getenv("PG_DATABASE")
self.username = os.getenv("PG_USER")
self.password = os.getenv("PG_PASSWORD")
self.host = os.getenv("PG_HOST")
self.port = os.getenv("PG_PORT")
self.engine = create_engine(
    f"postgresql+psycopg2://{self.username}:{self.password}@"
    f"{self.host}:{self.port}/{self.database}"
)
```

## Fetching Hex Data

The get_hex_data method is designed to fetch hex data from an API using specified date ranges. It employs a robust error handling mechanism to manage potential API exceptions and unexpected errors during data retrieval.
```python
def get_hex_data(self, date_from, date_to):
    params = {"date_from": date_from, "date_to": date_to}

    try:
        return self.api_client.get_data_request(self.api_endpoint, params=params)
    except APIClientException as e:
        self.logger.error(str(e))
        raise DataLoaderException("Failed to fetch data.") from None
    except Exception as e:
        self.logger.error(f"An unexpected error occurred: {str(e)}")
        raise DataLoaderException("An unexpected error occurred.") from None
```

## Retrieving Full Data from PostgreSQL
The `get_full_data` method allows retrieving complete data from a specified PostgreSQL table. This functionality enables data extraction from the database into a Pandas DataFrame, providing the flexibility to perform further analysis and manipulation.

```python
def get_full_data(self, table_name):
    try:
        with self.engine.connect() as connection:
            query = f"SELECT * FROM {table_name}"
            data_df = pd.read_sql(text(query), connection)
            return data_df
    except Exception as e:
        print(f"An error occurred while fetching data: {e}")
        return None
```


## Geo Data Lookup

The `get_hex_lookup` method, nested within the `DataLoader` class, is specialized for performing spatial joins between hex grid data and various boundary types, such as highstreets, town centres, bespoke areas, and business improvement districts (BIDs).

For each boundary type, the method follows a consistent process:

1. Loading the relevant shapefile using GeoPandas.
2. Constructing a SQL query to extract boundary data from the PostgreSQL database.
3. Utilizing the sjoin method to perform a spatial join between hex grid and boundary data.
4. Extracting the necessary columns from the spatial join result.

Here's an example for highstreets:
```python
if lookup_type == "highstreet":
    # ... (similar process for towncentre, bespoke, and bid)
    lookup_table["highstreet_id"] = lookup_table["highstreet_id"].astype("Int64")
    return lookup_table
```
This method effectively combines geographical and tabular data, producing a GeoDataFrame that associates hex IDs with boundary IDs.

## Summary

This comprehensive documentation provides an extensive understanding of the `DataLoader` class, its methods, and the `get_hex_lookup` method for geo data lookup. It covers the initialization process, data fetching capabilities, and data retrieval from PostgreSQL. Furthermore, it offers insights into spatial joins for associating hex grid data with various geographical boundaries. This documentation serves as a valuable resource for leveraging the data loading and geo data lookup functionalities in a sophisticated manner.

1. Import the required libraries and modules:

