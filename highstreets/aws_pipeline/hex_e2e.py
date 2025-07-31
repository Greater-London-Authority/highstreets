import os
import warnings
from datetime import datetime
from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_source_sink.gischangetracking import DataProcessor
from highstreets.data_transformation.hextransform import HexTransform
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

# Initialise data loader
data_loader = DataLoader()

# Get the start_date and end_date from environment variables
start_date = os.getenv('START_DATE')
end_date = os.getenv('END_DATE')

if not start_date or not end_date:
    raise ValueError(
        "Both START_DATE and END_DATE environment variables must be provided.")

# Debugging log for clarity
print(f"Making API request for BT hex 3-hourly data"
      f" with start_date: {start_date}, end_date: {end_date}")

# Retrieve BT footfall data using API within the specified date range
data = data_loader.get_hex_data(str(start_date), str(end_date))

# Initialize HexTransform for data transformation
hex_transform = HexTransform()

# Transform the received data
transformed_data = hex_transform.transform_data(data)

# Initialize DataWriter for data storage
data_writer = DataWriter()

# Append transformed data to PostgreSQL table
data_writer.append_data_to_postgres(transformed_data, "bt_footfall_tfl_hex_3hourly")

# get full range BT hex data from postgres and write to csv
tfl_hex_full_range = data_loader.get_full_data("bt_footfall_tfl_hex_3hourly")

# # Retrieve full range BT hex data from PostgreSQL and write to CSV
# data_writer.write_hex_to_csv_by_year(
#     tfl_hex_full_range,
#     f"{base_dir}bt/processed/hex_grid",
# )

# add here to offload hex data to s3
data_writer.export_table_by_year_to_s3(
    table_name='bt_footfall_tfl_hex_3hourly',
    date_column='count_date',
    s3_base_path=f"{base_dir}bt/processed/hex_grid",
    file_prefix='hex_3hourly_counts',
    latest=True
)

# Offloading latest year hex  data to london datastore - needs to be updated to
# to get the year automatically from the end_date
latest_year = datetime.strptime(end_date, "%Y-%m-%d").year
data_writer.upload_data_to_lds(
    slug="footfall-bt-people-counts-hsds",
    custom_date_column="count_date",
    resource_title=f"hex_3hourly_counts_{latest_year}.csv",
    file_path=(
        f"{base_dir}"
        f"bt/processed/hex_grid/"
        f"hex_3hourly_counts_{latest_year}.csv"
    ),
)
# sub-license westminster University-upload to lds
data_writer.upload_data_to_lds(
    slug="westminster-university",
    custom_date_column="count_date",
    resource_title=f"BT_3hourly_counts_{latest_year}.csv",
    file_path=(
        f"{base_dir}"
        f"bt/processed/hex_grid/"
        f"hex_3hourly_counts_{latest_year}.csv"
    ),
)

# Initialize DataProcessor to obtain new hex IDs from tracking table
data_processor = DataProcessor()
data_processor.process_changes()

# Town Centre transformation
hex_transform.fetch_and_transform_hex_data(
    transform_layer='hex_tc_transform_query.sql',
    table_name='econ_busyness_bt_towncentres_3hourly_counts',
    load_to_db=True,
    truncate=True
)

hex_transform.fetch_and_transform_hex_data(
    transform_layer='hex_hs_transform_query.sql',
    table_name='econ_busyness_bt_highstreets_3hourly_counts',
    load_to_db=True,
    truncate=True
)

hex_transform.fetch_and_transform_hex_data(
    transform_layer='hex_bid_transform_query.sql',
    table_name='econ_busyness_bt_bids_3hourly_counts',
    load_to_db=True,
    truncate=True
)

hex_transform.fetch_and_transform_hex_data(
    transform_layer='hex_bespoke_transform_query.sql',
    table_name='econ_busyness_bt_bespokes_3hourly_counts',
    load_to_db=True,
    truncate=True
)

hex_transform.concat_and_load_all_hex_layers(
    query_file='hex_all_layer_concat_query.sql',
    target_table='econ_busyness_bt_3hourly_counts',
    truncate=True,
    load_to_db=True
)

# Retrieve full range data from PostgreSQL
hs_full_range = data_loader.get_full_data("econ_busyness_bt_highstreets_3hourly_counts")
tc_full_range = data_loader.get_full_data("econ_busyness_bt_towncentres_3hourly_counts")
bid_full_range = data_loader.get_full_data("econ_busyness_bt_bids_3hourly_counts")
bespoke_full_range = data_loader.get_full_data(
    "econ_busyness_bt_bespokes" "_3hourly_counts"
)

data_writer.export_table_to_s3(table_name='econ_busyness_bt_bids_3hourly_counts',
                               s3_base_path=f"{base_dir}bt/processed/bid",
                               file_prefix='bid_3hourly_counts')
data_writer.export_table_to_s3(table_name='econ_busyness_bt_highstreets_3hourly_counts',
                               s3_base_path=f"{base_dir}bt/processed/highstreet",
                               file_prefix='highstreet_3hourly_counts')
data_writer.export_table_to_s3(table_name='econ_busyness_bt_towncentres_3hourly_counts',
                               s3_base_path=f"{base_dir}bt/processed/towncentre",
                               file_prefix='towncentre_3hourly_counts')
data_writer.export_table_to_s3(table_name='econ_busyness_bt_bespokes_3hourly_counts',
                               s3_base_path=f"{base_dir}bt/processed/bespoke",
                               file_prefix='bespoke_3hourly_counts')

# update data in London Datastore along with start and end dates
data_writer.upload_data_to_lds(
    slug="footfall-bt-people-counts-hsds",
    resource_title="highstreets_3hourly_counts.csv",
    file_path=(
        f"{base_dir}bt/processed/highstreet/highstreet_3hourly_counts.csv"
    ),
)
data_writer.upload_data_to_lds(
    slug="footfall-bt-people-counts-hsds",
    resource_title="towncentres_3hourly_counts.csv",
    file_path=(
        f"{base_dir}bt/processed/towncentre/towncentre_3hourly_counts.csv"
    ),
)
data_writer.upload_data_to_lds(
    slug="footfall-bt-people-counts-hsds",
    resource_title="bids_3hourly_counts.csv",
    file_path=(
        f"{base_dir}bt/processed/bid/bid_3hourly_counts.csv"
    ),
)
data_writer.upload_data_to_lds(
    slug="footfall-bt-people-counts-hsds",
    resource_title="bespokes_3hourly_counts.csv",
    file_path=(
        f"{base_dir}bt/processed/bespoke/bespoke_3hourly_counts.csv"
    ),
)

# sub-licensing agreement for colliers
# process HSDS data for the HOLBA sites
# select ids cooresponding to HOLBA sites
holba_ids = [112, 113, 114, 115, 116, 117, 118, 197]

# filtering all holba site footfall data and writing it to csv
bespoke_full_range[bespoke_full_range["bespoke_area_id"].isin(
    holba_ids)].assign(hours=lambda x: "'" + x["hours"]).to_csv(
    f"{base_dir}"
    "bt/processed/bespoke/Colliers agreement - Holba sites/"
    "colliers_hsds_bt_footfall_3hourly_counts.csv",
    index=False,
)

# Offloading Holba site data to datastore
data_writer.upload_data_to_lds(
    slug="colliers---hsds",
    resource_title="colliers_hsds_footfall_3hourly_counts.csv",
    df=bespoke_full_range[bespoke_full_range["bespoke_area_id"].isin(holba_ids)],
    file_path=(
        f"{base_dir}"
        "bt/processed/bespoke/"
        "Colliers agreement - Holba sites/"
        "colliers_hsds_bt_footfall_3hourly_counts.csv"
    ),
)

hex_bid_lookup = data_loader.get_full_data('econ_busyness_hex_bid_lookup')

columns_hex_bid = [
    "hex_id",
    "bid_name",
    "count_date",
    "day",
    "hours",
    "resident",
    "visitor",
    "worker",
    "loyalty_percentage",
    "dwell_time",
]

bid_full_range = data_loader.get_full_data("econ_busyness_bt_bids_3hourly_counts")

# sub license Jon Puleson BID
jon_puleson_ids = [46]

jon_puleson_hex = tfl_hex_full_range.merge(
    hex_bid_lookup[hex_bid_lookup["bid_id"].isin(jon_puleson_ids)],
    left_on="hex_id",
    right_on="hex_id",
    how="right",
)

columns_hex_bid = [
    "hex_id",
    "bid_name",
    "count_date",
    "day",
    "hours",
    "resident",
    "visitor",
    "worker",
    "loyalty_percentage",
    "dwell_time",
]

jon_puleson_hex.assign(hours=lambda x: "'" + x["time_indicator"])[
    columns_hex_bid].to_csv(
    f"{base_dir}bt/"
    "processed/hex_grid/jon_puleson/jon_puleson_bt_hex_3hourly_counts.csv",
    index=False,
)
# Offloading rendle intel harrow 3hourly hex counts data to datastore
data_writer.upload_data_to_lds(
    slug="jon-puleston-for-station-to-station-bid",
    resource_title="jon_puleson_bt_hex_3hourly_counts.csv",
    file_path=(
        f"{base_dir}bt/"
        "processed/hex_grid/jon_puleson/jon_puleson_bt_hex_3hourly_counts.csv"
    ),
)

# sublicnese - aveson young


hex_towncentre_lookup = data_loader.get_full_data("econ_busyness_hex_towncentre_lookup")

hex_bespoke_lookup = data_loader.get_full_data("econ_busyness_hex_bespoke_lookup")


aveson_young_tc_ids = [23, 33, 29, 37, 28, 46, 31]
aveson_young_bespoke_ids = [249]


aveson_hex_tc = tfl_hex_full_range.merge(
    hex_towncentre_lookup[hex_towncentre_lookup["tc_id"].isin(aveson_young_tc_ids)],
    left_on="hex_id",
    right_on="hex_id",
    how="right",
)
aveson_hex_bespoke = tfl_hex_full_range.merge(
    hex_bespoke_lookup[hex_bespoke_lookup["bespoke_area_id"].isin(
        aveson_young_bespoke_ids)],
    left_on="hex_id",
    right_on="hex_id",
    how="right",
)

columns_hex_tc = [
    "hex_id",
    "tc_name",
    "count_date",
    "day",
    "hours",
    "resident",
    "visitor",
    "worker",
    "loyalty_percentage",
    "dwell_time",
]

columns_hex_bespoke = [
    "hex_id",
    "name",
    "count_date",
    "day",
    "hours",
    "resident",
    "visitor",
    "worker",
    "loyalty_percentage",
    "dwell_time",
]

aveson_hex_tc.assign(hours=lambda x: "'" + x["time_indicator"])[columns_hex_tc].to_csv(
    f"{base_dir}"
    "bt/processed/hex_grid/avison_young/avison_tc_bt_hex_3hourly_counts.csv",
    index=False,
)
aveson_hex_bespoke.assign(hours=lambda x: "'" + x["time_indicator"])[
    columns_hex_bespoke
].to_csv(
    f"{base_dir}"
    "bt/processed/hex_grid/avison_young/avison_bespoke_bt_hex_3hourly_counts.csv",
    index=False,
)

# Offloading aveson 3hourly hex counts data to datastore
data_writer.upload_data_to_lds(
    slug="avison-young",
    resource_title="avison_tc_bt_hex_3hourly_counts.csv",
    file_path=(
        f"{base_dir}"
        "bt/processed/hex_grid/avison_young/avison_tc_bt_hex_3hourly_counts.csv"
    ),
)

# Offloading aveson 3hourly hex counts data to datastore
data_writer.upload_data_to_lds(
    slug="avison-young",
    resource_title="avison_bespoke_bt_hex_3hourly_counts.csv",
    file_path=(
        f"{base_dir}"
        "bt/processed/hex_grid/avison_young/avison_bespoke_bt_hex_3hourly_counts.csv"
    ),
)


# Sublicenses - Fitzrovia & Knightsbridge

fitzrovia_ids = [21, 77]
knightsbridge_ids = [64, 69]

fitzrovia_hex = tfl_hex_full_range.merge(
    hex_bid_lookup[hex_bid_lookup["bid_id"].isin(fitzrovia_ids)],
    left_on="hex_id",
    right_on="hex_id",
    how="right",
)
knightsbridge_hex = tfl_hex_full_range.merge(
    hex_bid_lookup[hex_bid_lookup["bid_id"].isin(knightsbridge_ids)],
    left_on="hex_id",
    right_on="hex_id",
    how="right",
)

columns_hex_bid = [
    "hex_id",
    "bid_name",
    "count_date",
    "day",
    "hours",
    "resident",
    "visitor",
    "worker",
    "loyalty_percentage",
    "dwell_time",
]

fitzrovia_hex.assign(hours=lambda x: "'" + x["time_indicator"])[columns_hex_bid].to_csv(
    f"{base_dir}"
    "bt/processed/hex_grid/fitzrovia/Fitzrovia_bt_hex_3hourly_counts.csv",
    index=False,
)
knightsbridge_hex.assign(hours=lambda x: "'" + x["time_indicator"])[
    columns_hex_bid
].to_csv(
    f"{base_dir}"
    "bt/processed/hex_grid/knightsbridge/Knightsbridge_bt_hex_3hourly_counts.csv",
    index=False,
)

# Offloading Fitzrovia 3hourly hex counts data to datastore
data_writer.upload_data_to_lds(
    slug="rendle-intelligence-for-fitzrovia-partnership",
    resource_title="Fitzrovia_bt_hex_3hourly_counts.csv",
    df=fitzrovia_hex,
    file_path=(
        f"{base_dir}"
        "bt/processed/hex_grid/fitzrovia/Fitzrovia_bt_hex_3hourly_counts.csv"
    ),
)

# Offloading Knightsbridge 3hourly hex counts data to datastore
data_writer.upload_data_to_lds(
    slug="rendle-intelligence-for-knightsbridge-partnership",
    resource_title="Knightsbridge_bt_hex_3hourly_counts.csv",
    df=knightsbridge_hex,
    file_path=(
        f"{base_dir}"
        "bt/processed/hex_grid/knightsbridge/Knightsbridge_bt_hex_3hourly_counts.csv"
    ),
)

# sublicense - rendle intel harrow

# to be added to the pipeline

# Sublicenses - Southbank BID

southbank_bid_id = [35]

southbank_hex = tfl_hex_full_range.merge(
    hex_bid_lookup[hex_bid_lookup["bid_id"].isin(southbank_bid_id)],
    left_on="hex_id",
    right_on="hex_id",
    how="right",
)

southbank_bid_id = [35]

southbank_hex.assign(hours=lambda x: "'" + x["time_indicator"])[columns_hex_bid].to_csv(
    f"{base_dir}"
    "bt/processed/hex_grid/southbank/Southbank_bt_hex_3hourly_counts.csv",
    index=False,
)

# Offloading Southbank bid 3hourly hex counts data to southbank datastore page
data_writer.upload_data_to_lds(
    slug="southbank-centre",
    resource_title="Southbank_bt_hex_3hourly_counts.csv",
    df=southbank_hex,
    file_path=(
        f"{base_dir}"
        "bt/processed/hex_grid/southbank/Southbank_bt_hex_3hourly_counts.csv"
    ),
)
