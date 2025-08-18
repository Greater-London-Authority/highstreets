import os
import warnings
from highstreets import config
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.dailytransform import DailyTransform
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
print(f"Making API request for daily totals data"
      f" with start_date: {start_date}, end_date: {end_date}")

# Retrieve BT footfall data using API within the specified date range
data = data_loader.get_bt_daily_aggregate_customer_shapes(str(start_date), str(end_date))

# Instantiate Daily transform
daily_transform = DailyTransform()
# daily total count - aggregated shapes api by BT
data = daily_transform.raw_bt_daily_preprocess_data(data)

# Instantiate data writer
data_writer = DataWriter()
data_writer.append_data_to_postgres(
    data, table_name="econ_busyness_bt_daily_agg_cust_raw")

data_daily_full_range = data_loader.get_full_data(
    "econ_busyness_bt_daily_agg_cust_raw")

# writing to csv by year
data_writer.write_hex_to_csv_by_year(
    data_daily_full_range,
    output_dir=f"{base_dir}bt/processed/daily",
    custom_file_name="BT_daily_agg_counts",
)

# sublicense - colliers
data_daily_full_range[(data_daily_full_range['poi_type'] == 'bids') & (
    data_daily_full_range['poi_name'] == 'Heart of London')].to_csv(
    f"{base_dir}bt/processed/daily/Colliers agreement"
    f" - Holba sites/colliers_bt_daily_agg_counts.csv", index=False
)

data_writer.upload_data_to_lds(
    slug="colliers---hsds",
    custom_date_column="count_date",
    resource_title="colliers_bt_daily_agg_counts.csv",
    file_path=(
        f"{base_dir}"
        f"bt/processed/daily/"
        f"Colliers agreement - Holba sites/colliers_bt_daily_agg_counts.csv"
    ),
)

# offloading to London datastore
data_writer.upload_data_to_lds(
    slug="footfall-bt-daily-people-counts-hsds",
    custom_date_column="count_date",
    resource_title="BT_daily_agg_counts_2022.csv",
    file_path=(
        f"{base_dir}"
        "bt/processed/daily/BT_daily_agg_counts_2022.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="footfall-bt-daily-people-counts-hsds",
    custom_date_column="count_date",
    resource_title="BT_daily_agg_counts_2023.csv",
    file_path=(
        f"{base_dir}"
        "bt/processed/daily/BT_daily_agg_counts_2023.csv"
    ),
)

data_writer.upload_data_to_lds(
    slug="footfall-bt-daily-people-counts-hsds",
    custom_date_column="count_date",
    resource_title="BT_daily_agg_counts_2024.csv",
    file_path=(
        f"{base_dir}"
        "bt/processed/daily/BT_daily_agg_counts_2024.csv"
    ),
)
data_writer.upload_data_to_lds(
    slug="footfall-bt-daily-people-counts-hsds",
    custom_date_column="count_date",
    resource_title="BT_daily_agg_counts_2025.csv",
    file_path=(
        f"{base_dir}"
        "bt/processed/daily/BT_daily_agg_counts_2025.csv"
    ),
)
