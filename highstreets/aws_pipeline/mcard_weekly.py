import pandas as pd
import logging
from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.mcard_transform import McardTransform

from highstreets.data_transformation.mcard_weekly_processor import FileProcessor
base_dir = config.BASE_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# instantiate the classes
data_loader = DataLoader()
data_writer = DataWriter()
mcard_transform = McardTransform()
dir_path = f"{base_dir}mastercard/sharefile_test"
mcard_weekly = FileProcessor(data_loader, data_writer, dir_path)

# Define table names
table_name_map = {
    # "15": "test_econ_busyness_mcard_raw_15_zoom",
    "18": "econ_busyness_mcard_raw_18_zoom"
}
# Ensure tables exist
for table_name in table_name_map.values():
    data_loader.create_table_mcard_weekly_raw(table_name)

# Process files
dir_path = "C:/Covid-19 Busyness/data/mastercard/sharefile"
file_processor = FileProcessor(data_loader, data_writer, dir_path)
file_processor.process_mcard_raw_files(table_name_map)


# Clean and process data
clean_table_name = "econ_busyness_mcard_clean_18_zoom"
cols = [
    "yr", "wk", "industry", "quad_id", "txn_amt", "txn_cnt", "acct_cnt",
    "avg_ticket", "avg_freq", "avg_spend_amt", "file_name",
    "weekday_weekend", "central_latitude", "central_longitude"
]
file_processor.clean_and_process_data(18, cols, clean_table_name)

file_processor.incremental_refresh_mcard_stg_18_zoom()

file_processor.process_inner_outer_weekly_summary()

# script for generating adjustment factor comes here
# script for generating adjustment factor comes here
# script for generating adjustment factor comes here


data_writer.export_table_to_s3(table_name='econ_busyness_mcard_adjustment_factors',
                               s3_base_path=(f"{base_dir}mastercard/spendingpulse/"
                                             f"adjustment_factor_historical_versions"),
                               file_prefix='adjustment_factor',
                               add_date_range_to_filename=True,
                               date_column='date')

# script for generating aggregated txn and yoy data comes here
# script for generating aggregated txn and yoy data comes here
# script for generating aggregated txn and yoy data comes here


# Define table mapping for different layers - upload to Postgres
table_mapping = {
    "txn_bespoke": "econ_busyness_mcard_bespoke_txn",
    "txn_bids": "econ_busyness_mcard_bids_txn",
    "txn_boroughs": "econ_busyness_mcard_boroughs_txn",
    "txn_caz": "econ_busyness_mcard_caz_txn",
    "txn_highstreets": "econ_busyness_mcard_highstreets_txn",
    "txn_inner_outer": "econ_busyness_mcard_inner_outer_txn",
    "txn_london": "econ_busyness_mcard_london_txn",
    "txn_msoas": "econ_busyness_mcard_msoas_txn",
    "txn_towncentres": "econ_busyness_mcard_towncentres_txn",
    "yoy_bespoke": "econ_busyness_mcard_bespoke_yoy",
    "yoy_bids": "econ_busyness_mcard_bids_yoy",
    "yoy_boroughs": "econ_busyness_mcard_boroughs_yoy",
    "yoy_caz": "econ_busyness_mcard_caz_yoy",
    "yoy_highstreets": "econ_busyness_mcard_highstreets_yoy",
    "yoy_inner_outer": "econ_busyness_mcard_inner_outer_yoy",
    "yoy_london": "econ_busyness_mcard_london_yoy",
    "yoy_msoas": "econ_busyness_mcard_msoas_yoy",
    "yoy_towncentres": "econ_busyness_mcard_towncentres_yoy"
}

# Dict of different layers (same as in your notebook)
mcard_weekly_layers = {
    "txn": [
        "bespoke", "bids", "boroughs", "caz",
        "highstreets", "inner_outer", "london", "msoas", "towncentres"
    ],
    "yoy": [
        "bespoke", "bids", "boroughs", "caz",
        "highstreets", "inner_outer", "london", "msoas", "towncentres"
    ]
}

print("Loading data to PostgreSQL tables...")

# Loop through each layer and load to PostgreSQL
for prefix, resources in mcard_weekly_layers.items():
    for resource in resources:
        file_key = f"{prefix}_{resource}"
        file_path = f"{base_dir}mastercard/weekly/processed/{file_key}.csv"
        table_name = table_mapping.get(file_key)

        if table_name:
            try:
                print(f"Loading {file_key} to {table_name}...")

                # Read CSV file
                df = pd.read_csv(file_path)
                print(f"  Read {len(df)} rows from {file_key}.csv")
                print(f"  CSV columns: {list(df.columns)}")

                # Load to PostgreSQL with column standardization
                data_writer.truncate_and_load_to_postgres(
                    dataframe=df,
                    table_name=table_name,
                    schema="gisapdata",
                    standardize_columns=True
                )
                print(f"  ✓ Successfully loaded {file_key} to {table_name}")

            except Exception as e:
                print(f"  ✗ Error loading {file_key}: {str(e)}")
                import traceback
                traceback.print_exc()
        else:
            print(f"  ⚠ No table mapping for: {file_key}")

print("\nPostgreSQL loading completed!")

# below few lines of code is reading mastercard weekly txn bespoke data
# And filtering the data for various users

# reading full range txn_bespoke data from mastercard directory
mcard_weekly = pd.read_csv(
    f"{base_dir}mastercard/weekly/processed/txn_bespoke.csv"
)

# sub-licensing agreement for colliers
# process HSDS data for the HOLBA sites
# select ids cooresponding to HOLBA sites
holba_ids = [112, 113, 114, 115, 116, 117, 118, 197]

# filtering all holba site weekly transaction data and writing it to csv
mcard_weekly[mcard_weekly["bespoke_area_id"].isin(holba_ids)].to_csv(
    f"{base_dir}"
    "mastercard/weekly/processed/bespoke/"
    "Colliers agreement - Holba sites/colliers_hsds_mcard_weekly_txn.csv",
    index=False,
)

# Offloading Holba Site 3hourly txn data to datastore
data_writer.upload_data_to_lds(
    slug="colliers---hsds",
    custom_date_column="week_start",
    resource_title="colliers_hsds_mcard_weekly_txn.csv",
    df=mcard_weekly[mcard_weekly["bespoke_area_id"].isin(holba_ids)],
    file_path=(
        f"{base_dir}"
        "mastercard/weekly/processed/bespoke/"
        "Colliers agreement - Holba sites/"
        "colliers_hsds_mcard_weekly_txn.csv"
    ),
)


# Sublicenses - Fitzrovia & Knightsbridge

fitzrovia_ids = [21, 77]
knightsbridge_ids = [64, 69]

# reading full range txn_bid data from mastercard directory
mcard_weekly_bid = pd.read_csv(
    f"{base_dir}mastercard/weekly/processed/txn_bids.csv"
)

# filtering all holba site weekly transaction data and writing it to csv
mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(fitzrovia_ids)].to_csv(
    f"{base_dir}"
    "mastercard/weekly/processed/bid/"
    "fitzrovia/Fitzrovia_hsds_mcard_weekly_txn.csv",
    index=False,
)
mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(knightsbridge_ids)].to_csv(
    f"{base_dir}"
    "mastercard/weekly/processed/bid/"
    "knightsbridge/Knightsbridge_hsds_mcard_weekly_txn.csv",
    index=False,
)

# Offloading Fitzrovia bid weekly txn data to datastore
data_writer.upload_data_to_lds(
    slug="rendle-intelligence-for-fitzrovia-partnership",
    custom_date_column="week_start",
    resource_title="Fitzrovia_hsds_mcard_weekly_txn.csv",
    df=mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(fitzrovia_ids)],
    file_path=(
        f"{base_dir}"
        "mastercard/weekly/processed/bid/fitzrovia/Fitzrovia_hsds_mcard_weekly_txn.csv"
    ),
)

# Offloading Knightsbridge bid weekly txn data to datastore
data_writer.upload_data_to_lds(
    slug="rendle-intelligence-for-knightsbridge-partnership",
    custom_date_column="week_start",
    resource_title="Knightsbridge_hsds_mcard_weekly_txn.csv",
    df=mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(knightsbridge_ids)],
    file_path=(
        f"{base_dir}"
        f"mastercard/weekly/processed/bid/knightsbridge/"
        f"Knightsbridge_hsds_mcard_weekly_txn.csv"
    ),
)

# dict of different layers
mcard_weekly_layers = {
    "txn": [
        "bespoke", "bids", "boroughs", "caz",
        "highstreets", "inner_outer", "london", "msoas", "towncentres"
    ],
    "yoy": [
        "bespoke", "bids", "boroughs", "caz",
        "highstreets", "inner_outer", "london", "msoas", "towncentres"
    ]
}

# Loop through each layer and upload to lds
for prefix, resources in mcard_weekly_layers.items():
    for resource in resources:
        resource_title = f"{prefix}_{resource}.csv"
        file_path = f"{base_dir}mastercard/weekly/processed/{resource_title}"
        data_writer.upload_data_to_lds(
            slug="mastercard-retail-location-insights",
            custom_date_column="week_start",
            resource_title=resource_title,
            file_path=file_path
        )

try:
    # Initialize the transformer
    # instantiate the classes
    data_loader = DataLoader()
    data_writer = DataWriter()
    dir_path = f"{base_dir}mastercard/sharefile_test"
    mcard_weekly = FileProcessor(data_loader, data_writer, dir_path)

    # Combine weekly transaction data from all layers
    # This will combine data from: bids, highstreets, towncentres,
    # caz, bespoke, boroughs
    logging.info("Starting combination of weekly transaction data...")
    mcard_weekly.concat_and_load_all_mcard_weekly_txn_layers(
        query_file='weekly_txn_all_layers_concat_query.sql',
        target_table='econ_busyness_mcard_txn',
        truncate=True,
        load_to_db=True
    )

    # Combine weekly year-over-year data from all layers
    logging.info("Starting combination of weekly year-over-year data...")
    mcard_weekly.concat_and_load_all_mcard_weekly_yoy_layers(
        query_file='weekly_yoy_all_layers_concat_query.sql',
        target_table='econ_busyness_mcard_yoy',
        truncate=True,
        load_to_db=True
    )

    logging.info("Successfully completed weekly data combination pipeline!")

except Exception as e:
    logging.error(f"Error in weekly data combination pipeline: {str(e)}")
    raise
