# End-to-End BT-Data Flow Documentation

Welcome to the comprehensive documentation for the end-to-end BT data flow in the provided Python code. This guide provides an in-depth understanding of the workflow involved in fetching, transforming, processing, and storing data.

## Table of Contents

1. [Introduction](#introduction)
2. [Initialization](#initialization)
3. [Fetching and Transforming Data](#fetching-and-transforming-data)
4. [Data Storage - PostgreSQL](#Data-storage-postgresql)
5. [Processing Data Changes](#processing-data-changes)
6. [Data Layer Separation and Data Merging](#data-layer-separation)
7. [3-hourly Aggregates Transformation](#aggregating-data)
8. [Conditional Transformations and Data Appending](#appending-data-to-postgresql)
9. [Data Appending with ID Check](#id-check)
10. [Data Concatenation and Final Append](#data-concat-final-append)
11. [CSV Data Export](#csv-data-export)
12. [Conclusion](#conclusion)

## Introduction

This document outlines the data engineering workflow implemented in the provided code. The workflow involves retrieving, transforming, and storing footfall data from various sources, including APIs, PostgreSQL databases, and CSV files. The goal is to process and organize the data into different layers and aggregate levels, and then append the transformed data to PostgreSQL tables for further analysis.

## Initialization

The process starts with importing the necessary libraries and modules:

```python
import os
import pandas as pd
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_source_sink.gischangetracking import DataProcessor
from highstreets.data_transformation.hextransform import HexTransform
```
## Fetching and Transforming Data
The workflow begins with the retrieval of footfall data using the DataLoader class, which interfaces with data sources and APIs. The following environment variables, START_DATE and END_DATE, are utilized to define the date range for data retrieval. The retrieved data is transformed using the HexTransform class. The initial transformation produces the transformed_data dataframe. This transformation includes various cleaning and reformatting operations to align the data for further processing.

1. Initialize the DataLoader class to fetch data from the data source:
```python
data_loader = DataLoader()
```

2. Retrieve the start and end dates from environment variables(User input taken in Jenkins):

```python
start_date = os.environ.get("START_DATE")
end_date = os.environ.get("END_DATE")
```
3. Fetch BT footfall data using API within the specified date range:

```python
data = data_loader.get_hex_data(str(start_date), str(end_date))
```
## Data Storage - PostgreSQL
The transformed data is stored in a PostgreSQL table named "bt_footfall_tfl_hex_3hourly" using the DataWriter class. This involves appending the transformed data to the existing table.

```python
data_writer.append_data_to_postgres(
    transformed_data, "bt_footfall_tfl_hex_3hourly"
)
```
## Processing Data Changes
The DataProcessor class is employed to manage changes in hex IDs. This is relevant when there are updates, additions, or deletions to hex IDs representing different types of layers. The process_changes() method effectively handles these changes.

```python
data_processor = DataProcessor()
data_processor.process_changes()
```
## Data Layer Separation and Data Merging
Hex IDs are categorized into distinct types (highstreets, town centres, bids, bespoke areas) using the DataProcessor class. The separated hex IDs are then merged with the full range BT hex data obtained from the PostgreSQL database.

```python
hs_hex_insert = pd.DataFrame(data_processor.hs_hex_ids, columns=["hex_id", "highstreet_id"])
tc_hex_insert = pd.DataFrame(data_processor.tc_hex_ids, columns=["hex_id", "tc_id"])
bid_hex_insert = pd.DataFrame(data_processor.bid_hex_ids, columns=["hex_id", "bid_id"])
bespoke_hex_insert = pd.DataFrame(data_processor.bespoke_hex_ids, columns=["hex_id", "bespoke_area_id"])

new_hs_data = hs_hex_insert.merge(tfl_hex_full_range, left_on="hex_id", right_on="hex_id", how="left")
# (similar operations for new_tc_data, new_bid_data, new_bespoke_data)
```
## 3-hourly Aggregates Transformation
The transformed data is aggregated into 3-hourly intervals for different layers. This step facilitates easier analysis and visualization. The HexTransform class provides methods for each type of layer to perform these transformations.

```python
hs = hex_transform.highstreet_threehourly_transform(transformed_data)
tc = hex_transform.towncentre_threehourly_transform(transformed_data)
bid = hex_transform.bid_threehourly_transform(transformed_data)
bespoke = hex_transform.bespoke_threehourly_transform(transformed_data)
```
## Conditional Transformations and Data Appending 
Conditional transformations are applied to new boundary data for each layer. These transformations involve creating dataframes based on specific conditions and merging them with the corresponding hex data from PostgreSQL.

```python
if not new_bespoke_data.empty:
    bespoke_new = hex_transform.bespoke_threehourly_transform(new_bespoke_data)
    # ... (filtering and merging operations)
if not new_hs_data.empty:
    hs_new = hex_transform.highstreet_threehourly_transform(new_hs_data)
    # ... (filtering and merging operations)
# (similar operations for tc_new and bid_new)
```

## Data Appending with ID Check
The append_data_with_id_check method of the DataWriter class is employed for data appending to PostgreSQL tables. This method ensures that data is appended only if specific IDs are present, maintaining data consistency.

## Data Concatenation and Final Append
Data from different layers are concatenated into a single dataframe named econ_busyness_bt_3hourly_counts. This dataframe contains the aggregated and transformed data for highstreets, town centres, bids, and bespoke areas. This concatenated data is then appended to the appropriate PostgreSQL table.

```python
econ_busyness_bt_3hourly_counts = pd.concat([...])
data_writer.append_data_to_postgres(
    econ_busyness_bt_3hourly_counts, "econ_busyness_bt_3hourly_counts"
)
```
## CSV Data Export
Lastly, data for different layers and transformed data is exported to CSV files using the write_threehourly_hs_to_csv method of the DataWriter class. This step allows data to be shared and used in other contexts.
```python
data_writer.write_threehourly_hs_to_csv(hs_full_range)
# (similar operations for tc_full_range, bid_full_range, bespoke_full_range)
```
## Conclusion
This comprehensive documentation provides an in-depth understanding of each step in the data engineering workflow. It explains the purpose, mechanisms, and significance of every code segment, ensuring a clear comprehension of the entire process from data retrieval to storage and export.
