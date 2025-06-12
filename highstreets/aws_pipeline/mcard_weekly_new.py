import pandas as pd
import os
from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.mcard_transform import McardTransform
from highstreets.core.sql_manager import SQLManager
from highstreets.data_transformation.mcard_weekly_processor import FileProcessor
from sqlalchemy import create_engine
import psycopg2
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

base_dir = config.BASE_DIR
# initialize the database connection
database = os.getenv("PG_DATABASE")
username = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@" f"{host}:{port}/{database}"
)

# instantiate the classes
data_loader = DataLoader()
data_writer = DataWriter()
mcard_transform = McardTransform()
sql_manager = SQLManager()
dir_path = f"{base_dir}mastercard/sharefile_test"
mcard_weekly = FileProcessor(data_loader, data_writer, dir_path)

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname=os.getenv("PG_DATABASE"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT"),
)

# Temporary paths to save data while testing
# Path to save the adjusted data
weekly_txn_dir = "Z:/HSDS/data/mastercard/weekly/processed/test/"
weekly_adj_path = weekly_txn_dir + "adjusted_weekly_data/"

# Do these steps happen elsewhere?

# Upload of files -> raw table (econ_busyness_mcard_raw_18_zoom)
# Raw table (econ_busyness_mcard_raw_18_zoom) -> clean table processing (econ_busyness_mcard_clean_18_zoom)
# Clean table (econ_busyness_mcard_clean_18_zoom) -> staging table processing (econ_busyness_mcard_stg_18_zoom)
# Staging table (econ_busyness_mcard_stg_18_zoom) -> inner outer aggregation (test_econ_busyness_mcard_inner_outer_txn)


# Adjustment Factor generation using Spending Pulse
mcard_weekly.create_adjustment_factor(
    table_name="test_econ_busyness_mcard_inner_outer_txn",  # is this still the right table?
    rolling_average_months=12,
    ffill_missing_dates=False,
    date_from=None,  # I added these for testing purposes
    date_to=None,
    update_pg_table=True,
)

# Create new weekly quad level table from the staging table with adjusted values
# This is in progress and needs testing
# mcard_transform.adjust_mcard_data_sql(
#     query="update_mcard_adjustment_weekly.sql",
#     table_name="econ_busyness_mcard_stg_18_zoom",
# )

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
    weekly_agg = pd.read_sql_query(query, conn)
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


# # below few lines of code is reading mastercard weekly txn bespoke data
# # And filtering the data for various users

# # reading full range txn_bespoke data from mastercard directory
# mcard_weekly = pd.read_csv(f"{base_dir}mastercard/weekly/processed/txn_bespoke.csv")

# # sub-licensing agreement for colliers
# # process HSDS data for the HOLBA sites
# # select ids cooresponding to HOLBA sites
# holba_ids = [112, 113, 114, 115, 116, 117, 118, 197]

# # filtering all holba site weekly transaction data and writing it to csv
# mcard_weekly[mcard_weekly["bespoke_area_id"].isin(holba_ids)].to_csv(
#     f"{base_dir}"
#     "mastercard/weekly/processed/bespoke/"
#     "Colliers agreement - Holba sites/colliers_hsds_mcard_weekly_txn.csv",
#     index=False,
# )

# # Offloading Holba Site 3hourly txn data to datastore
# data_writer.upload_data_to_lds(
#     slug="colliers---hsds",
#     custom_date_column="week_start",
#     resource_title="colliers_hsds_mcard_weekly_txn.csv",
#     df=mcard_weekly[mcard_weekly["bespoke_area_id"].isin(holba_ids)],
#     file_path=(
#         f"{base_dir}"
#         "mastercard/weekly/processed/bespoke/"
#         "Colliers agreement - Holba sites/"
#         "colliers_hsds_mcard_weekly_txn.csv"
#     ),
# )


# # Sublicenses - Fitzrovia & Knightsbridge

# fitzrovia_ids = [21, 77]
# knightsbridge_ids = [64, 69]

# # reading full range txn_bid data from mastercard directory
# mcard_weekly_bid = pd.read_csv(f"{base_dir}mastercard/weekly/processed/txn_bids.csv")

# # filtering all holba site weekly transaction data and writing it to csv
# mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(fitzrovia_ids)].to_csv(
#     f"{base_dir}"
#     "mastercard/weekly/processed/bid/"
#     "fitzrovia/Fitzrovia_hsds_mcard_weekly_txn.csv",
#     index=False,
# )
# mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(knightsbridge_ids)].to_csv(
#     f"{base_dir}"
#     "mastercard/weekly/processed/bid/"
#     "knightsbridge/Knightsbridge_hsds_mcard_weekly_txn.csv",
#     index=False,
# )

# # Offloading Fitzrovia bid weekly txn data to datastore
# data_writer.upload_data_to_lds(
#     slug="rendle-intelligence-for-fitzrovia-partnership",
#     custom_date_column="week_start",
#     resource_title="Fitzrovia_hsds_mcard_weekly_txn.csv",
#     df=mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(fitzrovia_ids)],
#     file_path=(
#         f"{base_dir}"
#         "mastercard/weekly/processed/bid/fitzrovia/Fitzrovia_hsds_mcard_weekly_txn.csv"
#     ),
# )

# # Offloading Knightsbridge bid weekly txn data to datastore
# data_writer.upload_data_to_lds(
#     slug="rendle-intelligence-for-knightsbridge-partnership",
#     custom_date_column="week_start",
#     resource_title="Knightsbridge_hsds_mcard_weekly_txn.csv",
#     df=mcard_weekly_bid[mcard_weekly_bid["bid_id"].isin(knightsbridge_ids)],
#     file_path=(
#         f"{base_dir}"
#         f"mastercard/weekly/processed/bid/knightsbridge/"
#         f"Knightsbridge_hsds_mcard_weekly_txn.csv"
#     ),
# )

# # dict of different layers
# mcard_weekly_layers = {
#     "txn": [
#         "bespoke",
#         "bids",
#         "boroughs",
#         "caz",
#         "highstreets",
#         "inner_outer",
#         "london",
#         "msoas",
#         "towncentres",
#     ],
#     "yoy": [
#         "bespoke",
#         "bids",
#         "boroughs",
#         "caz",
#         "highstreets",
#         "inner_outer",
#         "london",
#         "msoas",
#         "towncentres",
#     ],
# }

# # Loop through each layer and upload to lds
# for prefix, resources in mcard_weekly_layers.items():
#     for resource in resources:
#         resource_title = f"{prefix}_{resource}.csv"
#         file_path = f"{base_dir}mastercard/weekly/processed/{resource_title}"
#         data_writer.upload_data_to_lds(
#             slug="mastercard-retail-location-insights",
#             custom_date_column="week_start",
#             resource_title=resource_title,
#             file_path=file_path,
#         )
