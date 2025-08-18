import pandas as pd
import os
import warnings
import logging
from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.core.sql_manager import SQLManager
from sqlalchemy import create_engine
from highstreets.data_transformation.mcard_transform import McardTransform
from highstreets.data_transformation.mcard_weekly_processor import FileProcessor
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import exc as sa_exc
# Suppress GeoPandas GEOS version warnings
warnings.filterwarnings('ignore', message='.*Shapely GEOS version.*incompatible.*')
# Suppress SQLAlchemy XML column warnings
warnings.filterwarnings(
    'ignore', category=sa_exc.SAWarning, message='.*Did not recognize type.*xml.*')
# Optional: Suppress all SQLAlchemy warnings if needed
# warnings.filterwarnings('ignore', category=sa_exc.SAWarning)
print("Warning filters applied for cleaner output")
base_dir = config.BASE_DIR

load_dotenv(find_dotenv())
# initialize the database connection
database = os.getenv("PG_DATABASE")
username = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@" f"{host}:{port}/{database}"
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# instantiate the classes
data_loader = DataLoader()
data_writer = DataWriter()
mcard_transform = McardTransform()
sql_manager = SQLManager(engine=engine)

# Define table names
table_name_map = {
    # "15": "test_econ_busyness_mcard_raw_15_zoom",
    "18": "econ_busyness_mcard_raw_18_zoom"
}
# Ensure tables exist
for table_name in table_name_map.values():
    data_loader.create_table_mcard_weekly_raw(table_name)

# Process files
dir_path = f"{base_dir}mastercard/weekly/raw/mcard_staging/"
mcard_weekly = FileProcessor(data_loader, data_writer, dir_path)
mcard_weekly.process_mcard_raw_files(table_name_map)


# Clean and process data
clean_table_name = "econ_busyness_mcard_clean_18_zoom"
cols = [
    "yr", "wk", "industry", "quad_id", "txn_amt", "txn_cnt", "acct_cnt",
    "avg_ticket", "avg_freq", "avg_spend_amt", "file_name",
    "weekday_weekend", "central_latitude", "central_longitude"
]
mcard_weekly.clean_and_process_data(18, cols, clean_table_name)

mcard_weekly.incremental_refresh_mcard_stg_18_zoom()

mcard_weekly.process_inner_outer_weekly_summary()

# Adjustment Factor generation using Spending Pulse
mcard_weekly.create_adjustment_factor(
    table_name="econ_busyness_mcard_inner_outer_txn_pre_adj",
    rolling_average_months=12,
    ffill_missing_dates=False,
    date_from=None,  # I added these for testing purposes
    date_to=None,
    update_pg_table=True,
)

data_writer.export_table_to_s3(table_name='econ_busyness_mcard_adjustment_factors',
                               s3_base_path=(f"{base_dir}mastercard/spendingpulse/"
                                             f"adjustment_factor_historical_versions"),
                               file_prefix='adjustment_factor',
                               add_date_range_to_filename=True,
                               date_column='date')
# Path to save the adjusted data
weekly_txn_dir = f"{base_dir}mastercard/weekly/processed/"
weekly_adj_path = weekly_txn_dir + "adjusted_weekly_data/"

aggregation_ids_dict = {
    "inner_outer": {
        "poi_id": ["inner_outer"],
        "quad_area_lookup": "econ_busyness_mcard_inner_outer_quad_lookup",
    },
    "bid": {
        "poi_id": ["bid_id", "bid_name"],
        "quad_area_lookup": "econ_busyness_mcard_bids_quad_lookup",
    },
    "towncentre": {
        "poi_id": ["tc_id", "tc_name"],
        "quad_area_lookup": "econ_busyness_mcard_towncentres_quad_lookup",
    },
    "highstreet": {
        "poi_id": ["highstreet_id", "highstreet_name"],
        "quad_area_lookup": "econ_busyness_mcard_highstreets_quad_lookup",
    },
    "msoa": {
        "poi_id": ["msoa11cd", "msoa11nm"],
        "quad_area_lookup": "econ_busyness_mcard_msoas_quad_lookup",
    },
    "bespoke": {
        "poi_id": ["bespoke_area_id", "name"],
        "quad_area_lookup": "econ_busyness_mcard_bespoke_quad_lookup",
    },
    "borough": {
        "poi_id": ["gss_code", "name"],
        "quad_area_lookup": "econ_busyness_mcard_boroughs_quad_lookup",
    },
    "caz": {
        "poi_id": ["objectid", "name"],
        "quad_area_lookup": "econ_busyness_mcard_caz_quad_lookup",
    },
}


# --------------------------------------------------
# Aggreagte the weekly data
# --------------------------------------------------

for agg in aggregation_ids_dict.keys():
    # Load the area query and save to csv
    query = sql_manager.get_query(f"{agg}_weekly_query.sql")
    weekly_agg = sql_manager.execute_query(query)
    weekly_agg["week_start"] = pd.to_datetime(
        weekly_agg["week_start"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    weekly_agg.to_csv(
        f"{weekly_txn_dir}" f"mcard_weekly_{agg}_txn.csv",
        index=False,
    )


# --------------------------------------------------
# Adjust the aggregated data
# --------------------------------------------------


for agg in aggregation_ids_dict.keys():
    print(agg)
    if agg in ["highstreet", "bid", "towncentre", "msoa", "borough"]:
        save_name = f"{agg}s"
    else:
        save_name = agg

    adjusted_data = mcard_weekly.mcard_adjust_weekly(
        pd.read_csv(weekly_txn_dir + f"mcard_weekly_{agg}_txn.csv"),
        save_path=weekly_adj_path,
        col_to_adjust=[
            f"txn_amt_wd_{sector}"
            for sector in config.SECTORS_DF["geo_insights"].values
        ]
        + [
            f"txn_amt_we_{sector}"
            for sector in config.SECTORS_DF["geo_insights"].values
        ],
        date_col="week_start",
        lookup_file="econ_busyness_mcard_inner_outer_quad_lookup",
        quad_lookup_file=aggregation_ids_dict[agg]["quad_area_lookup"],
        poi_id=aggregation_ids_dict[agg]["poi_id"],
        filename=f"txn_{save_name}",
    )


# -----------------------------------------------------------------
# Create adjusted London level data (Combine Inner and Outer)
# -----------------------------------------------------------------

txn_io_london = pd.read_csv(f"{weekly_adj_path}txn_inner_outer.csv")
txn_london = (
    txn_io_london.groupby(["yr", "wk", "week_start"]).sum(min_count=1).reset_index()
)
txn_london["area"] = "London"
txn_london = txn_london[
    ["yr", "wk", "week_start", "area"]
    + [i for i in txn_io_london.columns if i.startswith("txn")]
]
txn_london = txn_london.round(3)

txn_london.to_csv(f"{weekly_adj_path}txn_london.csv", index=False)

# Create London YoY file
yoy = txn_london.copy()
for col in [i for i in yoy.columns if i.startswith("txn_")]:
    yoy = mcard_weekly.calculate_yoy_growth_compared_to_2019(
        yoy, col, f"yoy_{col}", ids=["area"]
    )
# Filter for 2019 onwards
yoy = yoy[yoy["yr"] >= 2019]
# Get all columns before txn_amt ones
first_txn_amt_col = [column for column in yoy.columns if column.startswith("txn_amt_")][
    0
]
id_cols = list(yoy.loc[:, :first_txn_amt_col].columns[:-1])
# Drop txn_ cols
yoy = yoy.drop(columns=[i for i in yoy.columns if i.startswith("txn_")]).round(2)

yoy = yoy[id_cols + [i for i in yoy.columns if i.startswith("yoy")]]
yoy.to_csv(f"{weekly_adj_path}yoy_london.csv", index=False)

print("Data adjusted")

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
        file_path = (f"{base_dir}mastercard/weekly/processed/"
                     f"adjusted_weekly_data/{file_key}.csv")
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
    f"{base_dir}mastercard/weekly/processed/adjusted_weekly_data/txn_bespoke.csv"
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


# Sublicenses - Knightsbridge

knightsbridge_ids = [64, 69]

# reading full range txn_bid data from mastercard directory
mcard_weekly_bid = pd.read_csv(
    f"{base_dir}mastercard/weekly/processed/adjusted_weekly_data/txn_bids.csv"
)

mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(knightsbridge_ids)].to_csv(
    f"{base_dir}"
    "mastercard/weekly/processed/bid/"
    "knightsbridge/Knightsbridge_hsds_mcard_weekly_txn.csv",
    index=False,
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
        file_path = (f"{base_dir}mastercard/weekly/processed/"
                     f"adjusted_weekly_data/{resource_title}")
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
