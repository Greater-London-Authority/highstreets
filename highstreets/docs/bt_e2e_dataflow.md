# End-to-End BT-Data Flow Documentation

Welcome to the comprehensive documentation for the end-to-end BT data flow in the provided Python code. This guide provides an in-depth understanding of the workflow involved in fetching, transforming, processing, and storing data.

## Table of Contents

- [Introduction](#introduction)
- [Parameters](#parameters)
- [Environment Variables](#environment-variables)
- [Installation Instructions](#installation-instructions)
- [Running the Jenkins Job](#running-the-jenkins-job)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)
- [Initialization](#initialization)
- [Fetching and Transforming Data](#fetching-and-transforming-data)
- [Data Storage - PostgreSQL](#data-storage-postgresql)
- [Processing Data Changes](#processing-data-changes)
- [Data Layer Separation and Data Merging](#data-layer-separation-and-data-merging)
- [3-hourly Aggregates Transformation](#aggregating-data)
- [Conditional Transformations and Data Appending](#conditional-transformations-and-data-appending)
- [Data Appending with ID Check](#data-appending-with-id-check)
- [Data Concatenation and Final Append](#data-concatenation-and-final-append)
- [CSV Data Export](#csv-data-export)
- [Conclusion](#conclusion)

## Introduction

This document outlines the data engineering workflow implemented in the provided code. The workflow involves retrieving, transforming, and storing footfall data from various sources, including APIs, PostgreSQL databases, and CSV files. The goal is to process and organize the data into different layers and aggregate levels, and then append the transformed data to PostgreSQL tables for further analysis.

The Jenkins job for this project automates various tasks using parameters provided by the user. Before you can run the job, it's essential to understand the available parameters and environment variables.

## Parameters

The Jenkins job accepts the following parameters:

- `START_DATE`: Enter the start date in the format `YYYY-MM-DD`. This sets the beginning of the date range for data processing.

- `END_DATE`: Enter the end date in the format `YYYY-MM-DD`. This sets the end of the date range for data processing.

## Environment Variables

The job uses the following environment variables, which must be configured in Jenkins:

- `CONSUMER_KEY`: This variable should store the consumer key securely using Jenkins credentials.

- `CONSUMER_SECRET`: This variable should store the consumer secret securely using Jenkins credentials.

- `PG_DATABASE`: The PostgreSQL database name.

- `PG_USER`: The PostgreSQL username.

- `PG_PASSWORD`: The PostgreSQL password.

- `PG_HOST`: The PostgreSQL host address.

- `PG_PORT`: The PostgreSQL port.

## Installation Instructions

Before running the Jenkins job, ensure that you have configured the necessary credentials in Jenkins:

1. **Consumer Key and Secret**:
   - Configure the `CONSUMER_KEY` and `CONSUMER_SECRET` in Jenkins credentials securely.

2. **PostgreSQL Credentials**:
   - Configure the `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`, `PG_HOST`, and `PG_PORT` credentials securely.

## Running the Jenkins Job

Follow these steps to run the Jenkins job with parameters:

1. **Navigate to Jenkins**:
   - Log in to your Jenkins instance.

2. **Locate the Job**:
   - Find the Jenkins job associated with this project named `HSDS-BT-E2E-DATAFLOW`.

3. **Start a New Build**:
   - Click on the job name and select "Build with Parameters" from the menu.

4. **Parameter Input**:
   - In the "Build with Parameters" section, you will find the following fields:
     - `START_DATE`: Enter the desired start date.
     - `END_DATE`: Enter the desired end date.

5. **Submit the Job**:
   - Click the "Build" button to start the job with the provided parameters.

## Examples

Here are some examples of valid parameter values:

- `START_DATE`: `2023-01-01`
- `END_DATE`: `2023-12-31`

## Troubleshooting

If you encounter any issues while running the Jenkins job, consider the following:

- **Invalid Parameters**: Ensure that you have entered valid `START_DATE` and `END_DATE` values in the correct format.

- **Credentials**: Verify that the necessary credentials (`CONSUMER_KEY`, `CONSUMER_SECRET`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`, `PG_HOST`, and `PG_PORT`) are correctly configured in Jenkins.

## More about the dataflow in detail:
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
