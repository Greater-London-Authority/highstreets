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

# add here to offload hex data to s3
data_writer.export_table_by_year_to_s3(
    table_name='bt_footfall_tfl_hex_3hourly',
    date_column='count_date',
    s3_base_path=f"{base_dir}bt/processed/hex_grid",
    file_prefix='hex_3hourly_counts',
    latest=True,
    apostrophe_columns=['time_indicator']
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

data_writer.export_table_to_s3(table_name='econ_busyness_bt_bids_3hourly_counts',
                               s3_base_path=f"{base_dir}bt/processed/bid",
                               file_prefix='bid_3hourly_counts',
                               apostrophe_columns=['hours'])
data_writer.export_table_to_s3(table_name='econ_busyness_bt_highstreets_3hourly_counts',
                               s3_base_path=f"{base_dir}bt/processed/highstreet",
                               file_prefix='highstreet_3hourly_counts',
                               apostrophe_columns=['hours'])
data_writer.export_table_to_s3(table_name='econ_busyness_bt_towncentres_3hourly_counts',
                               s3_base_path=f"{base_dir}bt/processed/towncentre",
                               file_prefix='towncentre_3hourly_counts',
                               apostrophe_columns=['hours'])
data_writer.export_table_to_s3(table_name='econ_busyness_bt_bespokes_3hourly_counts',
                               s3_base_path=f"{base_dir}bt/processed/bespoke",
                               file_prefix='bespoke_3hourly_counts',
                               apostrophe_columns=['hours'])

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
